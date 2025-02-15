using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerFunctionStore : IFunctionStore
{
    private readonly Func<Task<SqlConnection>> _connFunc;
    private readonly string _tableName;

    private readonly SqlServerTimeoutStore _timeoutStore;
    private readonly SqlServerEffectsStore _effectsStore;
    private readonly SqlServerMessageStore _messageStore;
    private readonly SqlServerCorrelationsStore _correlationStore;
    private readonly SqlServerTypeStore _typeStore;
    private readonly SqlServerMigrator _migrator;
    
    public IEffectsStore EffectsStore => _effectsStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    public ICorrelationStore CorrelationStore => _correlationStore;
    public ITypeStore TypeStore => _typeStore;
    public IMessageStore MessageStore => _messageStore;
    public Utilities Utilities { get; }
    public IMigrator Migrator => _migrator;
    private readonly SqlServerSemaphoreStore _semaphoreStore;
    public ISemaphoreStore SemaphoreStore => _semaphoreStore;

    private readonly SqlServerUnderlyingRegister _underlyingRegister;

    public SqlServerFunctionStore(string connectionString, string tablePrefix = "")
    {
        _tableName = tablePrefix == "" ? "RFunctions" : tablePrefix;
        
        _connFunc = CreateConnection(connectionString);
        _messageStore = new SqlServerMessageStore(connectionString, _tableName);
        _timeoutStore = new SqlServerTimeoutStore(connectionString, _tableName);
        _underlyingRegister = new SqlServerUnderlyingRegister(connectionString, _tableName);
        _effectsStore = new SqlServerEffectsStore(connectionString, _tableName);
        _correlationStore = new SqlServerCorrelationsStore(connectionString, _tableName);
        _semaphoreStore = new SqlServerSemaphoreStore(connectionString, _tableName);
        _typeStore = new SqlServerTypeStore(connectionString, _tableName);
        _migrator = new SqlServerMigrator(connectionString, _tableName);
        Utilities = new Utilities(_underlyingRegister);
    }
    
    private static Func<Task<SqlConnection>> CreateConnection(string connectionString)
    {
        return async () =>
        {
            var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return connection;
        };
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        var createTables = await _migrator.InitializeAndMigrate();
        if (!createTables)
            return;
        
        await _underlyingRegister.Initialize();
        await _messageStore.Initialize();
        await _effectsStore.Initialize();
        await _timeoutStore.Initialize();
        await _correlationStore.Initialize();
        await _typeStore.Initialize();
        await _semaphoreStore.Initialize();
        await using var conn = await _connFunc();
        _initializeSql ??= @$"    
            CREATE TABLE {_tableName} (
                FlowType INT NOT NULL,
                FlowInstance UNIQUEIDENTIFIER NOT NULL,
                Status INT NOT NULL,
                Epoch INT NOT NULL,               
                Expires BIGINT NOT NULL,
                Interrupted BIT NOT NULL DEFAULT 0,
                ParamJson VARBINARY(MAX) NULL,                                        
                ResultJson VARBINARY(MAX) NULL,
                ExceptionJson NVARCHAR(MAX) NULL,
                HumanInstanceId NVARCHAR(MAX) NOT NULL,                                                                        
                Timestamp BIGINT NOT NULL,
                Parent NVARCHAR(MAX) NULL,
                PRIMARY KEY (FlowType, FlowInstance)
            );
            CREATE INDEX {_tableName}_idx_Executing
                ON {_tableName} (Expires, FlowType, FlowInstance)
                INCLUDE (Epoch)
                WHERE Status = {(int)Status.Executing};           
            CREATE INDEX {_tableName}_idx_Postponed
                ON {_tableName} (Expires, FlowType, FlowInstance)
                INCLUDE (Epoch)
                WHERE Status = {(int)Status.Postponed};
            CREATE INDEX {_tableName}_idx_Succeeded
                ON {_tableName} (FlowType, FlowInstance)
                WHERE Status = {(int)Status.Succeeded};";

        await using var command = new SqlCommand(_initializeSql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    private string? _truncateSql;
    public async Task TruncateTables()
    {
        await _underlyingRegister.TruncateTable();
        await _messageStore.TruncateTable();
        await _timeoutStore.Truncate();
        await _effectsStore.Truncate();
        await _correlationStore.Truncate();
        await _typeStore.Truncate();
        await _semaphoreStore.Truncate();
        
        await using var conn = await _connFunc();
        _truncateSql ??= $"TRUNCATE TABLE {_tableName}";
        await using var command = new SqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _createFunctionSql;
    public async Task<bool> CreateFunction(
        StoredId storedId, 
        FlowInstance humanInstanceId,
        byte[]? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp,
        StoredId? parent)
    {
        await using var conn = await _connFunc();
        
        try
        {
            _createFunctionSql ??= @$"
                INSERT INTO {_tableName}(
                    FlowType, FlowInstance, 
                    ParamJson, 
                    Status,
                    Epoch, 
                    Expires,
                    Timestamp,
                    HumanInstanceId,
                    Parent
                )
                VALUES
                (
                    @FlowType, @flowInstance, 
                    @ParamJson,   
                    @Status,
                    0,
                    @Expires,
                    @Timestamp,
                    @HumanInstanceId,
                    @Parent
                )";

            await using var command = new SqlCommand(_createFunctionSql, conn);
            
            command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
            command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
            command.Parameters.AddWithValue("@Status", (int) (postponeUntil == null ? Status.Executing : Status.Postponed));
            command.Parameters.AddWithValue("@ParamJson", param == null ? SqlBinary.Null : param);
            command.Parameters.AddWithValue("@Expires", postponeUntil ?? leaseExpiration);
            command.Parameters.AddWithValue("@HumanInstanceId", humanInstanceId.Value);
            command.Parameters.AddWithValue("@Timestamp", timestamp);
            command.Parameters.AddWithValue("@Parent", parent?.Serialize() ?? (object) DBNull.Value);

            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException sqlException) when (sqlException.Number == SqlError.UNIQUENESS_VIOLATION)
        {
            return false;
        }

        return true;
    }

    private string? _bulkScheduleFunctionsSql;
    public async Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
    {
        _bulkScheduleFunctionsSql ??= @$"
            MERGE INTO {_tableName}
            USING (VALUES @VALUES) 
            AS source (
                FlowType, 
                FlowInstance, 
                ParamJson, 
                Status,
                Epoch, 
                Expires,
                Timestamp,
                HumanInstanceId,
                Parent
            )
            ON {_tableName}.FlowType = source.FlowType AND {_tableName}.flowInstance = source.flowInstance         
            WHEN NOT MATCHED THEN
              INSERT (FlowType, FlowInstance, ParamJson, Status, Epoch, Expires, Timestamp, HumanInstanceId, Parent)
              VALUES (source.FlowType, source.flowInstance, source.ParamJson, source.Status, source.Epoch, source.Expires, source.Timestamp, source.HumanInstanceId, source.Parent);";

        var parentStr = parent == null ? "NULL" : $"'{parent}'";
        var valueSql = $"(@FlowType, @FlowInstance, @ParamJson, {(int)Status.Postponed}, 0, 0, 0, @HumanInstanceId, {parentStr})";
        var chunk = functionsWithParam
            .Select(
                (fp, i) =>
                {
                    var sql = valueSql
                        .Replace("@FlowType", $"@FlowType{i}")
                        .Replace("@FlowInstance", $"@FlowInstance{i}")
                        .Replace("@ParamJson", $"@ParamJson{i}")
                        .Replace("@HumanInstanceId", $"@HumanInstanceId{i}");

                    return new { Id = i, Sql = sql, FunctionId = fp.StoredId, Param = fp.Param, HumanInstanceId = fp.HumanInstanceId };
                }).Chunk(100);

        await using var conn = await _connFunc();
        foreach (var idAndSqls in chunk)
        {
            var valuesSql = string.Join($",{Environment.NewLine}", idAndSqls.Select(a => a.Sql));
            var sql = _bulkScheduleFunctionsSql.Replace("@VALUES", valuesSql);
            
            await using var command = new SqlCommand(sql, conn);
            foreach (var idAndSql in idAndSqls)
            {
                command.Parameters.AddWithValue($"@FlowType{idAndSql.Id}", idAndSql.FunctionId.Type.Value);
                command.Parameters.AddWithValue($"@FlowInstance{idAndSql.Id}", idAndSql.FunctionId.Instance.Value);
                command.Parameters.AddWithValue($"@ParamJson{idAndSql.Id}", idAndSql.Param == null ? SqlBinary.Null : idAndSql.Param);
                command.Parameters.AddWithValue($"@HumanInstanceId{idAndSql.Id}", idAndSql.HumanInstanceId);
            }
            
            await command.ExecuteNonQueryAsync();
        }
    }

    private string? _restartExecutionSql;
    public async Task<StoredFlow?> RestartExecution(StoredId storedId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await _connFunc();
        _restartExecutionSql ??= @$"
            UPDATE {_tableName}
            SET Epoch = Epoch + 1, 
                Status = {(int)Status.Executing}, 
                Expires = @LeaseExpiration,
                Interrupted = 0
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance AND Epoch = @ExpectedEpoch;

            SELECT ParamJson,                
                   Status,
                   ResultJson, 
                   ExceptionJson,                   
                   Expires,
                   Epoch,
                   Interrupted,
                   Timestamp,
                   HumanInstanceId,
                   Parent
            FROM {_tableName}
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance";

        await using var command = new SqlCommand(_restartExecutionSql, conn);
        command.Parameters.AddWithValue("@LeaseExpiration", leaseExpiration);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        await using var reader = await command.ExecuteReaderAsync();
        if (reader.RecordsAffected == 0)
            return default;
        
        var sf = ReadToStoredFlow(storedId, reader);
        return sf?.Epoch == expectedEpoch + 1
            ? sf
            : default;
    }

    private string? _renewLeaseSql;
    public async Task<bool> RenewLease(StoredId storedId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await _connFunc();
        _renewLeaseSql ??= @$"
            UPDATE {_tableName}
            SET Expires = @Expires
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance AND Epoch = @Epoch";
        
        await using var command = new SqlCommand(_renewLeaseSql, conn);
        command.Parameters.AddWithValue("@Expires", leaseExpiration);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@Epoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
    
    public async Task<int> RenewLeases(IReadOnlyList<LeaseUpdate> leaseUpdates, long leaseExpiration)
    {
        await using var conn = await _connFunc();

        var predicates = leaseUpdates
            .Select(u =>
                $"(FlowType = {u.StoredId.Type.Value} AND FlowInstance = '{u.StoredId.Instance.Value}' AND Epoch = {u.ExpectedEpoch})"
            ).StringJoin(" OR " + Environment.NewLine);

        var sql = @$"
            UPDATE {_tableName}
            SET Expires = {leaseExpiration}
            WHERE {predicates}";
        
        await using var command = new SqlCommand(sql, conn);
        return await command.ExecuteNonQueryAsync();
    }

    private string? _getExpiredFunctionsSql;
    public async Task<IReadOnlyList<IdAndEpoch>> GetExpiredFunctions(long expiresBefore)
    {
        await using var conn = await _connFunc();
        _getExpiredFunctionsSql ??= @$"
            SELECT FlowType, FlowInstance, Epoch
            FROM {_tableName} WITH (NOLOCK) 
            WHERE Expires <= @Expires AND (Status = { (int)Status.Executing } OR Status = { (int)Status.Postponed})";

        await using var command = new SqlCommand(_getExpiredFunctionsSql, conn);
        command.Parameters.AddWithValue("@Expires", expiresBefore);

        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<IdAndEpoch>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var flowType = reader.GetInt32(0);
                var flowInstance = reader.GetGuid(1);
                var flowId = new StoredId(new StoredType(flowType), flowInstance.ToStoredInstance());
                var epoch = reader.GetInt32(2);
                rows.Add(new IdAndEpoch(flowId, epoch));    
            }

            reader.NextResult();
        }

        return rows;
    }

    private string? _getSucceededFunctionsSql;
    public async Task<IReadOnlyList<StoredInstance>> GetSucceededFunctions(StoredType storedType, long completedBefore)
    {
        await using var conn = await _connFunc();
        _getSucceededFunctionsSql ??= @$"
            SELECT FlowInstance
            FROM {_tableName} 
            WHERE FlowType = @FlowType 
              AND Status = {(int) Status.Succeeded} 
              AND Timestamp <= @CompletedBefore";

        await using var command = new SqlCommand(_getSucceededFunctionsSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedType.Value);
        command.Parameters.AddWithValue("@CompletedBefore", completedBefore);

        await using var reader = await command.ExecuteReaderAsync();
        var storedInstances = new List<StoredInstance>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var storedInstance = reader.GetGuid(0).ToStoredInstance();
                storedInstances.Add(storedInstance);    
            }

            reader.NextResult();
        }

        return storedInstances;
    }

    private string? _setFunctionStateSql;
    public async Task<bool> SetFunctionState(
        StoredId storedId, Status status, 
        byte[]? param, byte[]? result, 
        StoredException? storedException, 
        long expires,
        int expectedEpoch)
    {
        await using var conn = await _connFunc();
    
        _setFunctionStateSql ??= @$"
            UPDATE {_tableName}
            SET
                Status = @Status,
                ParamJson = @ParamJson,             
                ResultJson = @ResultJson,
                ExceptionJson = @ExceptionJson,
                Expires = @Expires,
                Epoch = Epoch + 1
            WHERE FlowType = @FlowType
            AND FlowInstance = @FlowInstance
            AND Epoch = @ExpectedEpoch";
        
        await using var command = new SqlCommand(_setFunctionStateSql, conn);
        command.Parameters.AddWithValue("@Status", (int) status);
        command.Parameters.AddWithValue("@ParamJson", param == null ? SqlBinary.Null : param);
        command.Parameters.AddWithValue("@ResultJson", result == null ? SqlBinary.Null : result);
        var exceptionJson = storedException == null ? null : JsonSerializer.Serialize(storedException);
        command.Parameters.AddWithValue("@ExceptionJson", exceptionJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@Expires", expires);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    private string? _succeedFunctionSql;
    public async Task<bool> SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await _connFunc();
        
        _succeedFunctionSql ??= @$"
            UPDATE {_tableName}
            SET Status = {(int) Status.Succeeded}, ResultJson = @ResultJson, Timestamp = @Timestamp, Epoch = @ExpectedEpoch
            WHERE FlowType = @FlowType
            AND FlowInstance = @FlowInstance
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(_succeedFunctionSql, conn);
        command.Parameters.AddWithValue("@ResultJson", result == null ? SqlBinary.Null : result);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    private string? _postponedFunctionSql;
    public async Task<bool> PostponeFunction(
        StoredId storedId, 
        long postponeUntil, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await _connFunc();
        
        _postponedFunctionSql ??= @$"
            UPDATE {_tableName}
            SET Status = {(int) Status.Postponed}, Expires = @PostponedUntil, Timestamp = @Timestamp, Epoch = @ExpectedEpoch
            WHERE FlowType = @FlowType
            AND FlowInstance = @FlowInstance
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(_postponedFunctionSql, conn);
        command.Parameters.AddWithValue("@PostponedUntil", postponeUntil);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    private string? _failFunctionSql;
    public async Task<bool> FailFunction(
        StoredId storedId, 
        StoredException storedException, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        
        await using var conn = await _connFunc();
        
        _failFunctionSql ??= @$"
            UPDATE {_tableName}
            SET Status = {(int) Status.Failed}, ExceptionJson = @ExceptionJson, Timestamp = @timestamp, Epoch = @ExpectedEpoch
            WHERE FlowType = @FlowType
            AND FlowInstance = @FlowInstance
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(_failFunctionSql, conn);
        command.Parameters.AddWithValue("@ExceptionJson", JsonSerializer.Serialize(storedException));
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    private string? _suspendFunctionSql;
    public async Task<bool> SuspendFunction(
        StoredId storedId, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await _connFunc();

        _suspendFunctionSql ??= @$"
                UPDATE {_tableName}
                SET Status = {(int)Status.Suspended}, Timestamp = @Timestamp
                WHERE FlowType = @FlowType AND 
                      FlowInstance = @FlowInstance AND                       
                      Epoch = @ExpectedEpoch AND
                      Interrupted = 0;";

        await using var command = new SqlCommand(_suspendFunctionSql, conn);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    private string? _interruptSql;
    private string? _interruptIfExecutingSql;
    public async Task<bool> Interrupt(StoredId storedId, bool onlyIfExecuting)
    {
        await using var conn = await _connFunc();
        _interruptSql ??= @$"
                UPDATE {_tableName}
                SET 
                    Interrupted = 1,
                    Status = 
                        CASE 
                            WHEN Status = {(int) Status.Suspended} THEN {(int) Status.Postponed}
                            ELSE Status
                        END,
                    Expires = 
                        CASE
                            WHEN Status = {(int) Status.Postponed} THEN 0
                            WHEN Status = {(int) Status.Suspended} THEN 0
                            ELSE Expires
                        END
                WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance";
        _interruptIfExecutingSql ??= _interruptSql + $" AND Status = {(int) Status.Executing}";

        var sql = onlyIfExecuting
            ? _interruptIfExecutingSql
            : _interruptSql;
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _interruptsSql;
    public async Task Interrupt(IEnumerable<StoredId> storedIds)
    {
        await using var conn = await _connFunc();
        _interruptsSql ??= @$"
                UPDATE {_tableName}
                SET 
                    Interrupted = 1,
                    Status = 
                        CASE 
                            WHEN Status = {(int) Status.Suspended} THEN {(int) Status.Postponed}
                            ELSE Status
                        END,
                    Expires = 
                        CASE
                            WHEN Status = {(int) Status.Postponed} THEN 0
                            WHEN Status = {(int) Status.Suspended} THEN 0
                            ELSE Expires
                        END
                WHERE @CONDITIONALS";

        var conditionals = storedIds
            .Select(storedId => $"(FlowType = {storedId.Type.Value} AND FlowInstance = '{storedId.Instance.Value}')")
            .StringJoin(" OR ");

        var sql = _interruptsSql.Replace("@CONDITIONALS", conditionals);

        await using var cmd = new SqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private string? _setParametersSql;
    public async Task<bool> SetParameters(
        StoredId storedId,
        byte[]? param, byte[]? result,
        int expectedEpoch)
    {
        await using var conn = await _connFunc();
        
        _setParametersSql ??= @$"
            UPDATE {_tableName}
            SET ParamJson = @ParamJson,  
                ResultJson = @ResultJson,
                Epoch = Epoch + 1
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(_setParametersSql, conn);
        command.Parameters.AddWithValue("@ParamJson", param == null ? SqlBinary.Null : param);
        command.Parameters.AddWithValue("@ResultJson", result == null ? SqlBinary.Null : result);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
    
    private string? _interruptedSql;
    public async Task<bool?> Interrupted(StoredId storedId)
    {
        await using var conn = await _connFunc();
        _interruptedSql ??= @$"
                SELECT Interrupted 
                FROM {_tableName}            
                WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance;";

        await using var command = new SqlCommand(_interruptedSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);

        var interrupted = await command.ExecuteScalarAsync();
        return (bool?) interrupted;
    }

    private string? _getFunctionStatusSql;
    public async Task<StatusAndEpoch?> GetFunctionStatus(StoredId storedId)
    {
        await using var conn = await _connFunc();
        _getFunctionStatusSql ??= @$"
            SELECT Status, Epoch
            FROM {_tableName}
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance";
        
        await using var command = new SqlCommand(_getFunctionStatusSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows)
            while (reader.Read())
            {
                var status = (Status) reader.GetInt32(0);
                var epoch = reader.GetInt32(1);

                return new StatusAndEpoch(status, epoch);
            }

        return null;
    }

    public async Task<IReadOnlyList<StatusAndEpochWithId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds)
    {
        var predicates = storedIds
            .Select(s => new { Type = s.Type.Value, Instance = s.Instance.Value })
            .GroupBy(id => id.Type, id => id.Instance)
            .Select(g => $"(FlowType = {g.Key} AND FlowInstance IN ({string.Join(",", g.Select(instance => $"'{instance}'"))}))")
            .StringJoin(" OR " + Environment.NewLine);

        var sql = @$"
            SELECT FlowType, FlowInstance, Status, Epoch
            FROM {_tableName}
            WHERE {predicates}";
        
        await using var conn = await _connFunc();
        
        await using var command = new SqlCommand(sql, conn);
        await using var reader = await command.ExecuteReaderAsync();
        var toReturn = new List<StatusAndEpochWithId>();
        
        while (reader.Read())
        {
            var type = reader.GetInt32(0).ToStoredType();
            var instance = reader.GetGuid(1).ToStoredInstance();
            var status = (Status) reader.GetInt32(2);
            var epoch = reader.GetInt32(3);

            var storedId = new StoredId(type, instance);
            toReturn.Add(new StatusAndEpochWithId(storedId, status, epoch));
        }

        return toReturn;
    }

    private string? _getFunctionSql;
    public async Task<StoredFlow?> GetFunction(StoredId storedId)
    {
        await using var conn = await _connFunc();
        _getFunctionSql ??= @$"
            SELECT  ParamJson, 
                    Status,
                    ResultJson, 
                    ExceptionJson,
                    Expires,
                    Epoch, 
                    Interrupted,
                    Timestamp,
                    HumanInstanceId,
                    Parent
            FROM {_tableName}
            WHERE FlowType = @FlowType
            AND flowInstance = @FlowInstance";
        
        await using var command = new SqlCommand(_getFunctionSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        
        await using var reader = await command.ExecuteReaderAsync();
        return ReadToStoredFlow(storedId, reader);
    }

    private string? _getInstancesWithStatusSql;
    public async Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType, Status status)
    {
        await using var conn = await _connFunc();
        _getInstancesWithStatusSql ??= @$"
            SELECT FlowInstance
            FROM {_tableName} 
            WHERE FlowType = @FlowType AND Status = @Status";

        await using var command = new SqlCommand(_getInstancesWithStatusSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedType.Value);
        command.Parameters.AddWithValue("@Status", (int) status);

        await using var reader = await command.ExecuteReaderAsync();
        var instances = new List<StoredInstance>(); 
        while (reader.Read())
        {
            var flowInstance = reader.GetGuid(0).ToStoredInstance();
            instances.Add(flowInstance);    
        }

        return instances;
    }

    private string? _getInstancesSql;
    public async Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType)
    {
        await using var conn = await _connFunc();
        _getInstancesSql ??= @$"
            SELECT FlowInstance
            FROM {_tableName} 
            WHERE FlowType = @FlowType";

        await using var command = new SqlCommand(_getInstancesSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedType.Value);

        await using var reader = await command.ExecuteReaderAsync();
        var instances = new List<StoredInstance>();
        while (reader.Read())
        {
            var flowInstance = reader.GetGuid(0);
            instances.Add(flowInstance.ToStoredInstance());
        }

        return instances;
    }

    private StoredFlow? ReadToStoredFlow(StoredId storedId, SqlDataReader reader)
    {
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var parameter = reader.IsDBNull(0) ? null : (byte[]) reader.GetValue(0);
                var status = (Status) reader.GetInt32(1);
                var result = reader.IsDBNull(2) ? null : (byte[]) reader.GetValue(2);
                var exceptionJson = reader.IsDBNull(3) ? null : reader.GetString(3);
                var storedException = exceptionJson == null
                    ? null
                    : JsonSerializer.Deserialize<StoredException>(exceptionJson);
                var expires = reader.GetInt64(4);
                var epoch = reader.GetInt32(5);
                var interrupted = reader.GetBoolean(6);
                var timestamp = reader.GetInt64(7);
                var humanInstanceId = reader.GetString(8);
                var parentId = reader.IsDBNull(9) ? null : StoredId.Deserialize(reader.GetString(9));

                return new StoredFlow(
                    storedId,
                    humanInstanceId,
                    parameter,
                    status,
                    result,
                    storedException,
                    epoch,
                    expires,
                    timestamp,
                    interrupted,
                    parentId
                );
            }
        }

        return default;
    }
    
    public async Task<bool> DeleteFunction(StoredId storedId)
    {
        await _messageStore.Truncate(storedId);
        await _effectsStore.Remove(storedId);
        await _timeoutStore.Remove(storedId);
        await _correlationStore.RemoveCorrelations(storedId);

        return await DeleteStoredFunction(storedId);
    }

    private string? _deleteFunctionSql;
    private async Task<bool> DeleteStoredFunction(StoredId storedId)
    {
        await using var conn = await _connFunc();
        _deleteFunctionSql ??= @$"
            DELETE FROM {_tableName}
            WHERE FlowType = @FlowType
            AND FlowInstance = @FlowInstance ";
        
        await using var command = new SqlCommand(_deleteFunctionSql, conn);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        
        return await command.ExecuteNonQueryAsync() == 1;
    }
}
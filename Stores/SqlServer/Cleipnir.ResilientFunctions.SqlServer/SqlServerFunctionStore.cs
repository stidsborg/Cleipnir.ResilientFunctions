using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
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
    private readonly SqlServerStatesStore _statesStore;
    private readonly SqlServerMessageStore _messageStore;
    private readonly SqlServerCorrelationsStore _correlationStore;
    private readonly SqlServerMigrator _migrator;
    
    public IEffectsStore EffectsStore => _effectsStore;
    public IStatesStore StatesStore => _statesStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    public ICorrelationStore CorrelationStore => _correlationStore;
    public IMessageStore MessageStore => _messageStore;
    public Utilities Utilities { get; }
    public IMigrator Migrator => _migrator;
    
    private readonly SqlServerUnderlyingRegister _underlyingRegister;

    public SqlServerFunctionStore(string connectionString, string tablePrefix = "")
    {
        _tableName = tablePrefix == "" ? "RFunctions" : tablePrefix;
        
        _connFunc = CreateConnection(connectionString);
        _messageStore = new SqlServerMessageStore(connectionString, _tableName);
        _timeoutStore = new SqlServerTimeoutStore(connectionString, _tableName);
        _underlyingRegister = new SqlServerUnderlyingRegister(connectionString, _tableName);
        _effectsStore = new SqlServerEffectsStore(connectionString, _tableName);
        _statesStore = new SqlServerStatesStore(connectionString, _tableName);
        _correlationStore = new SqlServerCorrelationsStore(connectionString, _tableName);
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
        await _statesStore.Initialize();
        await _timeoutStore.Initialize();
        await _correlationStore.Initialize();
        await using var conn = await _connFunc();
        _initializeSql ??= @$"    
            CREATE TABLE {_tableName} (
                FlowType NVARCHAR(200) NOT NULL,
                flowInstance NVARCHAR(200) NOT NULL,
                Status INT NOT NULL,
                Epoch INT NOT NULL,               
                Expires BIGINT NOT NULL,
                InterruptCount BIGINT NOT NULL DEFAULT 0,
                ParamJson NVARCHAR(MAX) NULL,                                        
                ResultJson NVARCHAR(MAX) NULL,
                DefaultStateJson NVARCHAR(MAX) NULL,
                ExceptionJson NVARCHAR(MAX) NULL,                                                                        
                Timestamp BIGINT NOT NULL,
                PRIMARY KEY (FlowType, flowInstance)
            );
            CREATE INDEX {_tableName}_idx_Executing
                ON {_tableName} (Expires, FlowType, flowInstance)
                INCLUDE (Epoch)
                WHERE Status = {(int)Status.Executing};           
            CREATE INDEX {_tableName}_idx_Postponed
                ON {_tableName} (Expires, FlowType, flowInstance)
                INCLUDE (Epoch)
                WHERE Status = {(int)Status.Postponed};
            CREATE INDEX {_tableName}_idx_Succeeded
                ON {_tableName} (FlowType, flowInstance)
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
        await _statesStore.Truncate();
        await _correlationStore.Truncate();
        
        await using var conn = await _connFunc();
        _truncateSql ??= $"TRUNCATE TABLE {_tableName}";
        await using var command = new SqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _createFunctionSql;
    public async Task<bool> CreateFunction(
        FlowId flowId, 
        string? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp)
    {
        await using var conn = await _connFunc();
        
        try
        {
            _createFunctionSql ??= @$"
                INSERT INTO {_tableName}(
                    FlowType, flowInstance, 
                    ParamJson, 
                    Status,
                    Epoch, 
                    Expires,
                    Timestamp)
                VALUES(
                    @FlowType, @flowInstance, 
                    @ParamJson,   
                    @Status,
                    0,
                    @Expires,
                    @Timestamp
                )";

            await using var command = new SqlCommand(_createFunctionSql, conn);
            
            command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
            command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
            command.Parameters.AddWithValue("@Status", (int) (postponeUntil == null ? Status.Executing : Status.Postponed));
            command.Parameters.AddWithValue("@ParamJson", param == null ? DBNull.Value : param);
            command.Parameters.AddWithValue("@Expires", postponeUntil ?? leaseExpiration);
            command.Parameters.AddWithValue("@Timestamp", timestamp);

            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException sqlException) when (sqlException.Number == SqlError.UNIQUENESS_VIOLATION)
        {
            return false;
        }

        return true;
    }

    private string? _bulkScheduleFunctionsSql;
    public async Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam)
    {
        _bulkScheduleFunctionsSql ??= @$"
            MERGE INTO {_tableName}
            USING (VALUES @VALUES) 
            AS source (
                FlowType, 
                flowInstance, 
                ParamJson, 
                Status,
                Epoch, 
                Expires,
                Timestamp
            )
            ON {_tableName}.FlowType = source.FlowType AND {_tableName}.flowInstance = source.flowInstance         
            WHEN NOT MATCHED THEN
              INSERT (FlowType, flowInstance, ParamJson, Status, Epoch, Expires, Timestamp)
              VALUES (source.FlowType, source.flowInstance, source.ParamJson, source.Status, source.Epoch, source.Expires, source.Timestamp);";

        var valueSql = $"(@FlowType, @flowInstance, @ParamJson, {(int)Status.Postponed}, 0, 0, 0)";
        var chunk = functionsWithParam
            .Select(
                (fp, i) =>
                {
                    var sql = valueSql
                        .Replace("@FlowType", $"@FlowType{i}")
                        .Replace("@flowInstance", $"@flowInstance{i}")
                        .Replace("@ParamJson", $"@ParamJson{i}");

                    return new { Id = i, Sql = sql, FunctionId = fp.FlowId, Param = fp.Param };
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
                command.Parameters.AddWithValue($"@flowInstance{idAndSql.Id}", idAndSql.FunctionId.Instance.Value);
                command.Parameters.AddWithValue($"@ParamJson{idAndSql.Id}", idAndSql.Param == null ? DBNull.Value : idAndSql.Param);
            }
            
            await command.ExecuteNonQueryAsync();
        }
    }

    private string? _restartExecutionSql;
    public async Task<StoredFlow?> RestartExecution(FlowId flowId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await _connFunc();
        _restartExecutionSql ??= @$"
            UPDATE {_tableName}
            SET Epoch = Epoch + 1, 
                Status = {(int)Status.Executing}, 
                Expires = @LeaseExpiration
            WHERE FlowType = @FlowType AND flowInstance = @flowInstance AND Epoch = @ExpectedEpoch;

            SELECT ParamJson,                
                   Status,
                   ResultJson, 
                   DefaultStateJson,
                   ExceptionJson,                   
                   Expires,
                   Epoch,
                   InterruptCount,
                   Timestamp
            FROM {_tableName}
            WHERE FlowType = @FlowType
            AND flowInstance = @flowInstance";

        await using var command = new SqlCommand(_restartExecutionSql, conn);
        command.Parameters.AddWithValue("@LeaseExpiration", leaseExpiration);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        await using var reader = await command.ExecuteReaderAsync();
        if (reader.RecordsAffected == 0)
            return default;
        
        var sf = ReadToStoredFlow(flowId, reader);
        return sf?.Epoch == expectedEpoch + 1
            ? sf
            : default;
    }

    private string? _renewLeaseSql;
    public async Task<bool> RenewLease(FlowId flowId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await _connFunc();
        _renewLeaseSql ??= @$"
            UPDATE {_tableName}
            SET Expires = @Expires
            WHERE FlowType = @FlowType AND flowInstance = @flowInstance AND Epoch = @Epoch";
        
        await using var command = new SqlCommand(_renewLeaseSql, conn);
        command.Parameters.AddWithValue("@Expires", leaseExpiration);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
        command.Parameters.AddWithValue("@Epoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
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
                var flowType = reader.GetString(0);
                var flowInstance = reader.GetString(1);
                var flowId = new FlowId(flowType, flowInstance);
                var epoch = reader.GetInt32(2);
                rows.Add(new IdAndEpoch(flowId, epoch));    
            }

            reader.NextResult();
        }

        return rows;
    }

    private string? _getSucceededFunctionsSql;
    public async Task<IReadOnlyList<FlowInstance>> GetSucceededFunctions(FlowType flowType, long completedBefore)
    {
        await using var conn = await _connFunc();
        _getSucceededFunctionsSql ??= @$"
            SELECT FlowInstance
            FROM {_tableName} 
            WHERE FlowType = @FlowType 
              AND Status = {(int) Status.Succeeded} 
              AND Timestamp <= @CompletedBefore";

        await using var command = new SqlCommand(_getSucceededFunctionsSql, conn);
        command.Parameters.AddWithValue("@FlowType", flowType.Value);
        command.Parameters.AddWithValue("@CompletedBefore", completedBefore);

        await using var reader = await command.ExecuteReaderAsync();
        var flowInstances = new List<FlowInstance>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var flowInstance = reader.GetString(0);
                flowInstances.Add(flowInstance);    
            }

            reader.NextResult();
        }

        return flowInstances;
    }

    private string? _setFunctionStateSql;
    public async Task<bool> SetFunctionState(
        FlowId flowId, Status status, 
        string? param, string? result, 
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
            AND flowInstance = @flowInstance
            AND Epoch = @ExpectedEpoch";
        
        await using var command = new SqlCommand(_setFunctionStateSql, conn);
        command.Parameters.AddWithValue("@Status", (int) status);
        command.Parameters.AddWithValue("@ParamJson", param == null ? DBNull.Value : param);
        command.Parameters.AddWithValue("@ResultJson", result == null ? DBNull.Value : result);
        var exceptionJson = storedException == null ? null : JsonSerializer.Serialize(storedException);
        command.Parameters.AddWithValue("@ExceptionJson", exceptionJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@Expires", expires);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    private string? _succeedFunctionSql;
    public async Task<bool> SucceedFunction(
        FlowId flowId, 
        string? result, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await _connFunc();
        
        _succeedFunctionSql ??= @$"
            UPDATE {_tableName}
            SET Status = {(int) Status.Succeeded}, ResultJson = @ResultJson, DefaultStateJson = @DefaultStateJson, Timestamp = @Timestamp, Epoch = @ExpectedEpoch
            WHERE FlowType = @FlowType
            AND flowInstance = @flowInstance
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(_succeedFunctionSql, conn);
        command.Parameters.AddWithValue("@ResultJson", result == null ? DBNull.Value : result);
        command.Parameters.AddWithValue("@DefaultStateJson", defaultState ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    private string? _postponedFunctionSql;
    public async Task<bool> PostponeFunction(
        FlowId flowId, 
        long postponeUntil, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await _connFunc();
        
        _postponedFunctionSql ??= @$"
            UPDATE {_tableName}
            SET Status = {(int) Status.Postponed}, Expires = @PostponedUntil, DefaultStateJson = @DefaultState, Timestamp = @Timestamp, Epoch = @ExpectedEpoch
            WHERE FlowType = @FlowType
            AND flowInstance = @flowInstance
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(_postponedFunctionSql, conn);
        command.Parameters.AddWithValue("@PostponedUntil", postponeUntil);
        command.Parameters.AddWithValue("@DefaultState", defaultState ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    private string? _failFunctionSql;
    public async Task<bool> FailFunction(
        FlowId flowId, 
        StoredException storedException, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        
        await using var conn = await _connFunc();
        
        _failFunctionSql ??= @$"
            UPDATE {_tableName}
            SET Status = {(int) Status.Failed}, ExceptionJson = @ExceptionJson, DefaultStateJson = @DefaultStateJson, Timestamp = @timestamp, Epoch = @ExpectedEpoch
            WHERE FlowType = @FlowType
            AND flowInstance = @flowInstance
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(_failFunctionSql, conn);
        command.Parameters.AddWithValue("@ExceptionJson", JsonSerializer.Serialize(storedException));
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@DefaultStateJson", defaultState ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    private string? _suspendFunctionSql;
    public async Task<bool> SuspendFunction(
        FlowId flowId, 
        long expectedInterruptCount, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await _connFunc();

        _suspendFunctionSql ??= @$"
                UPDATE {_tableName}
                SET Status = {(int)Status.Suspended}, DefaultStateJson = @DefaultStateJson, Timestamp = @Timestamp
                WHERE FlowType = @FlowType AND 
                      flowInstance = @flowInstance AND                       
                      Epoch = @ExpectedEpoch AND
                      InterruptCount = @ExpectedInterruptCount;";

        await using var command = new SqlCommand(_suspendFunctionSql, conn);
        command.Parameters.AddWithValue("@DefaultStateJson", defaultState ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);
        command.Parameters.AddWithValue("@ExpectedInterruptCount", expectedInterruptCount);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _setDefaultStateSql;
    public async Task SetDefaultState(FlowId flowId, string? stateJson)
    {
        await using var conn = await _connFunc();

        _setDefaultStateSql ??= @$"
                UPDATE {_tableName}
                SET DefaultStateJson = @DefaultStateJson
                WHERE FlowType = @FlowType AND flowInstance = @flowInstance";

        await using var command = new SqlCommand(_setDefaultStateSql, conn);
        command.Parameters.AddWithValue("@DefaultStateJson", stateJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);

        await command.ExecuteNonQueryAsync();
    }

    public async Task<bool> Interrupt(FlowId flowId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
                UPDATE {_tableName}
                SET 
                    InterruptCount = InterruptCount + 1,
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
                WHERE FlowType = @FlowType AND flowInstance = @FlowInstance;";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", flowId.Instance.Value);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _setParametersSql;
    public async Task<bool> SetParameters(
        FlowId flowId,
        string? param, string? result,
        int expectedEpoch)
    {
        await using var conn = await _connFunc();
        
        _setParametersSql ??= @$"
            UPDATE {_tableName}
            SET ParamJson = @ParamJson,  
                ResultJson = @ResultJson,
                Epoch = Epoch + 1
            WHERE FlowType = @FlowType AND flowInstance = @flowInstance AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(_setParametersSql, conn);
        command.Parameters.AddWithValue("@ParamJson", param == null ? DBNull.Value : param);
        command.Parameters.AddWithValue("@ResultJson", result == null ? DBNull.Value : result);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    private string? _incrementInterruptCountSql;
    public async Task<bool> IncrementInterruptCount(FlowId flowId)
    {
        await using var conn = await _connFunc();
        _incrementInterruptCountSql ??= @$"
                UPDATE {_tableName}
                SET InterruptCount = InterruptCount + 1
                WHERE FlowType = @FlowType AND flowInstance = @flowInstance AND Status = {(int) Status.Executing};";

        await using var command = new SqlCommand(_incrementInterruptCountSql, conn);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _getInterruptCountSql;
    public async Task<long?> GetInterruptCount(FlowId flowId)
    {
        await using var conn = await _connFunc();
        _getInterruptCountSql ??= @$"
                SELECT InterruptCount 
                FROM {_tableName}            
                WHERE FlowType = @FlowType AND flowInstance = @flowInstance;";

        await using var command = new SqlCommand(_getInterruptCountSql, conn);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);

        var interruptCount = await command.ExecuteScalarAsync();
        return (long?) interruptCount;
    }

    private string? _getFunctionStatusSql;
    public async Task<StatusAndEpoch?> GetFunctionStatus(FlowId flowId)
    {
        await using var conn = await _connFunc();
        _getFunctionStatusSql ??= @$"
            SELECT Status, Epoch
            FROM {_tableName}
            WHERE FlowType = @FlowType AND flowInstance = @flowInstance";
        
        await using var command = new SqlCommand(_getFunctionStatusSql, conn);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
        
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

    private string? _getFunctionSql;
    public async Task<StoredFlow?> GetFunction(FlowId flowId)
    {
        await using var conn = await _connFunc();
        _getFunctionSql ??= @$"
            SELECT  ParamJson, 
                    Status,
                    ResultJson, 
                    DefaultStateJson,
                    ExceptionJson,
                    Expires,
                    Epoch, 
                    InterruptCount,
                    Timestamp
            FROM {_tableName}
            WHERE FlowType = @FlowType
            AND flowInstance = @flowInstance";
        
        await using var command = new SqlCommand(_getFunctionSql, conn);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
        
        await using var reader = await command.ExecuteReaderAsync();
        return ReadToStoredFlow(flowId, reader);
    }

    private string? _getInstancesWithStatusSql;
    public async Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType, Status status)
    {
        await using var conn = await _connFunc();
        _getInstancesWithStatusSql ??= @$"
            SELECT FlowInstance
            FROM {_tableName} 
            WHERE FlowType = @FlowType AND Status = @Status";

        await using var command = new SqlCommand(_getInstancesWithStatusSql, conn);
        command.Parameters.AddWithValue("@FlowType", flowType.Value);
        command.Parameters.AddWithValue("@Status", (int) status);

        await using var reader = await command.ExecuteReaderAsync();
        var instances = new List<FlowInstance>(); 
        while (reader.Read())
        {
            var flowInstance = reader.GetString(0);
            instances.Add(flowInstance);    
        }

        return instances;
    }

    private string? _getInstancesSql;
    public async Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType)
    {
        await using var conn = await _connFunc();
        _getInstancesSql ??= @$"
            SELECT FlowInstance
            FROM {_tableName} 
            WHERE FlowType = @FlowType";

        await using var command = new SqlCommand(_getInstancesSql, conn);
        command.Parameters.AddWithValue("@FlowType", flowType.Value);

        await using var reader = await command.ExecuteReaderAsync();
        var instances = new List<FlowInstance>();
        while (reader.Read())
        {
            var flowInstance = reader.GetString(0);
            instances.Add(flowInstance);
        }

        return instances;
    }

    private string? _getTypesSql;
    public async Task<IReadOnlyList<FlowType>> GetTypes()
    {
        await using var conn = await _connFunc();
        _getTypesSql ??= $"SELECT DISTINCT(FlowType) FROM {_tableName}";

        await using var command = new SqlCommand(_getTypesSql, conn);

        await using var reader = await command.ExecuteReaderAsync();
        var flowTypes = new List<FlowType>();
        while (reader.Read())
        {
            var flowType = reader.GetString(0);
            flowTypes.Add(flowType);
        }

        return flowTypes;
    }

    private StoredFlow? ReadToStoredFlow(FlowId flowId, SqlDataReader reader)
    {
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var parameter = reader.IsDBNull(0) ? null : reader.GetString(0);
                var status = (Status) reader.GetInt32(1);
                var result = reader.IsDBNull(2) ? null : reader.GetString(2);
                var defaultStateJson = reader.IsDBNull(3) ? null : reader.GetString(3);
                var exceptionJson = reader.IsDBNull(4) ? null : reader.GetString(4);
                var storedException = exceptionJson == null
                    ? null
                    : JsonSerializer.Deserialize<StoredException>(exceptionJson);
                var expires = reader.GetInt64(5);
                var epoch = reader.GetInt32(6);
                var interruptCount = reader.GetInt64(7);
                var timestamp = reader.GetInt64(8);

                return new StoredFlow(
                    flowId,
                    parameter,
                    defaultStateJson,
                    status,
                    result,
                    storedException,
                    epoch,
                    expires,
                    timestamp,
                    interruptCount
                );
            }
        }

        return default;
    }
    
    public async Task<bool> DeleteFunction(FlowId flowId)
    {
        await _messageStore.Truncate(flowId);
        await _effectsStore.Remove(flowId);
        await _statesStore.Remove(flowId);
        await _timeoutStore.Remove(flowId);
        await _correlationStore.RemoveCorrelations(flowId);

        return await DeleteStoredFunction(flowId);
    }

    private string? _deleteFunctionSql;
    private async Task<bool> DeleteStoredFunction(FlowId flowId)
    {
        await using var conn = await _connFunc();
        _deleteFunctionSql ??= @$"
            DELETE FROM {_tableName}
            WHERE FlowType = @FlowType
            AND flowInstance = @flowInstance ";
        
        await using var command = new SqlCommand(_deleteFunctionSql, conn);
        command.Parameters.AddWithValue("@flowInstance", flowId.Instance.Value);
        command.Parameters.AddWithValue("@FlowType", flowId.Type.Value);
        
        return await command.ExecuteNonQueryAsync() == 1;
    }
}
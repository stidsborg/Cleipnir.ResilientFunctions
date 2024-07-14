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
    private readonly string _tablePrefix;

    private readonly SqlServerTimeoutStore _timeoutStore;
    private readonly SqlServerEffectsStore _effectsStore;
    private readonly SqlServerStatesStore _statesStore;
    private readonly SqlServerMessageStore _messageStore;
    private readonly SqlServerCorrelationsStore _correlationStore;
    
    public IEffectsStore EffectsStore => _effectsStore;
    public IStatesStore StatesStore => _statesStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    public ICorrelationStore CorrelationStore => _correlationStore;
    public IMessageStore MessageStore => _messageStore;
    public Utilities Utilities { get; }
    private readonly SqlServerUnderlyingRegister _underlyingRegister;

    public SqlServerFunctionStore(string connectionString, string tablePrefix = "")
    {
        _tablePrefix = tablePrefix == "" ? "RFunctions" : tablePrefix;
        
        _connFunc = CreateConnection(connectionString);
        _messageStore = new SqlServerMessageStore(connectionString, _tablePrefix);
        _timeoutStore = new SqlServerTimeoutStore(connectionString, _tablePrefix);
        _underlyingRegister = new SqlServerUnderlyingRegister(connectionString, _tablePrefix);
        _effectsStore = new SqlServerEffectsStore(connectionString, _tablePrefix);
        _statesStore = new SqlServerStatesStore(connectionString, _tablePrefix);
        _correlationStore = new SqlServerCorrelationsStore(connectionString, _tablePrefix);
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
        await _underlyingRegister.Initialize();
        await _messageStore.Initialize();
        await _effectsStore.Initialize();
        await _statesStore.Initialize();
        await _timeoutStore.Initialize();
        await _correlationStore.Initialize();
        await using var conn = await _connFunc();
        _initializeSql ??= @$"    
            CREATE TABLE {_tablePrefix} (
                FunctionTypeId NVARCHAR(200) NOT NULL,
                FunctionInstanceId NVARCHAR(200) NOT NULL,
                ParamJson NVARCHAR(MAX) NULL,                        
                Status INT NOT NULL,
                ResultJson NVARCHAR(MAX) NULL,
                DefaultStateJson NVARCHAR(MAX) NULL,
                ExceptionJson NVARCHAR(MAX) NULL,
                PostponedUntil BIGINT NULL,            
                Epoch INT NOT NULL,
                LeaseExpiration BIGINT NOT NULL,
                InterruptCount BIGINT NOT NULL DEFAULT 0,
                Timestamp BIGINT NOT NULL,
                PRIMARY KEY (FunctionTypeId, FunctionInstanceId)
            );
            CREATE INDEX {_tablePrefix}_idx_Executing
                ON {_tablePrefix} (FunctionTypeId, LeaseExpiration, FunctionInstanceId)
                INCLUDE (Epoch)
                WHERE Status = {(int)Status.Executing};
            CREATE INDEX {_tablePrefix}_idx_Postponed
                ON {_tablePrefix} (FunctionTypeId, PostponedUntil, FunctionInstanceId)
                INCLUDE (Epoch)
                WHERE Status = {(int)Status.Postponed};
            CREATE INDEX {_tablePrefix}_idx_Succeeded
                ON {_tablePrefix} (FunctionTypeId, FunctionInstanceId)
                WHERE Status = {(int)Status.Succeeded};";

        await using var command = new SqlCommand(_initializeSql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    private string? _dropIfExistsSql;
    public async Task DropIfExists()
    {
        await _underlyingRegister.DropUnderlyingTable();
        await _messageStore.DropUnderlyingTable();
        await _timeoutStore.DropUnderlyingTable();
        
        await using var conn = await _connFunc();
        _dropIfExistsSql ??= $"DROP TABLE IF EXISTS {_tablePrefix}";
        await using var command = new SqlCommand(_dropIfExistsSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await _underlyingRegister.TruncateTable();
        await _messageStore.TruncateTable();
        await _timeoutStore.Truncate();
        await _effectsStore.Truncate();
        await _statesStore.Truncate();
        await _correlationStore.Truncate();
        
        await using var conn = await _connFunc();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}";
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
                INSERT INTO {_tablePrefix}(
                    FunctionTypeId, FunctionInstanceId, 
                    ParamJson, 
                    Status,
                    Epoch, 
                    LeaseExpiration,
                    PostponedUntil,
                    Timestamp)
                VALUES(
                    @FunctionTypeId, @FunctionInstanceId, 
                    @ParamJson,   
                    @Status,
                    0, 
                    @LeaseExpiration,
                    @PostponeUntil,
                    @Timestamp
                )";

            await using var command = new SqlCommand(_createFunctionSql, conn);
            
            command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
            command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);
            command.Parameters.AddWithValue("@Status", (int) (postponeUntil == null ? Status.Executing : Status.Postponed));
            command.Parameters.AddWithValue("@ParamJson", param == null ? DBNull.Value : param);
            command.Parameters.AddWithValue("@LeaseExpiration", leaseExpiration);
            command.Parameters.AddWithValue("@PostponeUntil", postponeUntil == null ? DBNull.Value : postponeUntil.Value);
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
    public async Task BulkScheduleFunctions(IEnumerable<FunctionIdWithParam> functionsWithParam)
    {
        _bulkScheduleFunctionsSql ??= @$"
            MERGE INTO {_tablePrefix}
            USING (VALUES @VALUES) 
            AS source (
                FunctionTypeId, 
                FunctionInstanceId, 
                ParamJson, 
                Status,
                Epoch, 
                LeaseExpiration,
                PostponedUntil,
                Timestamp
            )
            ON {_tablePrefix}.FunctionTypeId = source.FunctionTypeId AND {_tablePrefix}.FunctionInstanceId = source.FunctionInstanceId         
            WHEN NOT MATCHED THEN
              INSERT (FunctionTypeId, FunctionInstanceId, ParamJson, Status, Epoch, LeaseExpiration, PostponedUntil, Timestamp)
              VALUES (source.FunctionTypeId, source.FunctionInstanceId, source.ParamJson, source.Status, source.Epoch, source.LeaseExpiration, source.PostponedUntil, source.Timestamp);";

        var valueSql = $"(@FunctionTypeId, @FunctionInstanceId, @ParamJson, {(int)Status.Postponed}, 0, 0, 0, 0)";
        var chunk = functionsWithParam
            .Select(
                fp =>
                {
                    var id = Guid.NewGuid().ToString("N");
                    var sql = valueSql
                        .Replace("@FunctionTypeId", $"@FunctionTypeId{id}")
                        .Replace("@FunctionInstanceId", $"@FunctionInstanceId{id}")
                        .Replace("@ParamJson", $"@ParamJson{id}");

                    return new { Id = id, Sql = sql, FunctionId = fp.FlowId, Param = fp.Param };
                }).Chunk(100);

        await using var conn = await _connFunc();
        foreach (var idAndSqls in chunk)
        {
            var valuesSql = string.Join($",{Environment.NewLine}", idAndSqls.Select(a => a.Sql));
            var sql = _bulkScheduleFunctionsSql.Replace("@VALUES", valuesSql);
            
            await using var command = new SqlCommand(sql, conn);
            foreach (var idAndSql in idAndSqls)
            {
                command.Parameters.AddWithValue($"@FunctionTypeId{idAndSql.Id}", idAndSql.FunctionId.Type.Value);
                command.Parameters.AddWithValue($"@FunctionInstanceId{idAndSql.Id}", idAndSql.FunctionId.Instance.Value);
                command.Parameters.AddWithValue($"@ParamJson{idAndSql.Id}", idAndSql.Param == null ? DBNull.Value : idAndSql.Param);
            }
            
            await command.ExecuteNonQueryAsync();
        }
    }

    private string? _restartExecutionSql;
    public async Task<StoredFunction?> RestartExecution(FlowId flowId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await _connFunc();
        _restartExecutionSql ??= @$"
            UPDATE {_tablePrefix}
            SET Epoch = Epoch + 1, 
                Status = {(int)Status.Executing}, 
                LeaseExpiration = @LeaseExpiration
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @ExpectedEpoch;

            SELECT ParamJson,                
                   Status,
                   ResultJson, 
                   DefaultStateJson,
                   ExceptionJson,
                   PostponedUntil,
                   Epoch, 
                   LeaseExpiration,
                   InterruptCount,
                   Timestamp
            FROM {_tablePrefix}
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId";

        await using var command = new SqlCommand(_restartExecutionSql, conn);
        command.Parameters.AddWithValue("@LeaseExpiration", leaseExpiration);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        await using var reader = await command.ExecuteReaderAsync();
        if (reader.RecordsAffected == 0)
            return default;
        
        var sf = ReadToStoredFunction(flowId, reader);
        return sf?.Epoch == expectedEpoch + 1
            ? sf
            : default;
    }

    private string? _renewLeaseSql;
    public async Task<bool> RenewLease(FlowId flowId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await _connFunc();
        _renewLeaseSql ??= @$"
            UPDATE {_tablePrefix}
            SET LeaseExpiration = @LeaseExpiration
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @Epoch";
        
        await using var command = new SqlCommand(_renewLeaseSql, conn);
        command.Parameters.AddWithValue("@LeaseExpiration", leaseExpiration);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);
        command.Parameters.AddWithValue("@Epoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    private string? _getCrashedFunctionsSql;
    public async Task<IReadOnlyList<InstanceIdAndEpoch>> GetCrashedFunctions(FlowType flowType, long leaseExpiresBefore)
    {
        await using var conn = await _connFunc();
        _getCrashedFunctionsSql ??= @$"
            SELECT FunctionInstanceId, Epoch
            FROM {_tablePrefix} WITH (NOLOCK)
            WHERE FunctionTypeId = @FunctionTypeId AND LeaseExpiration < @LeaseExpiration AND Status = {(int) Status.Executing}";

        await using var command = new SqlCommand(_getCrashedFunctionsSql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", flowType.Value);
        command.Parameters.AddWithValue("@LeaseExpiration", leaseExpiresBefore);

        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<InstanceIdAndEpoch>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var functionInstanceId = reader.GetString(0);
                var epoch = reader.GetInt32(1);
                rows.Add(new InstanceIdAndEpoch(functionInstanceId, epoch));    
            }

            reader.NextResult();
        }

        return rows;
    }

    private string? _getPostponedFunctionsSql;
    public async Task<IReadOnlyList<InstanceIdAndEpoch>> GetPostponedFunctions(FlowType flowType, long isEligibleBefore)
    {
        await using var conn = await _connFunc();
        _getPostponedFunctionsSql ??= @$"
            SELECT FunctionInstanceId, Epoch
            FROM {_tablePrefix} 
            WHERE FunctionTypeId = @FunctionTypeId 
              AND Status = {(int) Status.Postponed} 
              AND PostponedUntil <= @PostponedUntil";

        await using var command = new SqlCommand(_getPostponedFunctionsSql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", flowType.Value);
        command.Parameters.AddWithValue("@PostponedUntil", isEligibleBefore);

        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<InstanceIdAndEpoch>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var functionInstanceId = reader.GetString(0);
                var epoch = reader.GetInt32(1);
                rows.Add(new InstanceIdAndEpoch(functionInstanceId, epoch));    
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
            SELECT FunctionInstanceId
            FROM {_tablePrefix} 
            WHERE FunctionTypeId = @FunctionTypeId 
              AND Status = {(int) Status.Succeeded} 
              AND Timestamp <= @CompletedBefore";

        await using var command = new SqlCommand(_getSucceededFunctionsSql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", flowType.Value);
        command.Parameters.AddWithValue("@CompletedBefore", completedBefore);

        await using var reader = await command.ExecuteReaderAsync();
        var functionInstanceIds = new List<FlowInstance>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var functionInstanceId = reader.GetString(0);
                functionInstanceIds.Add(functionInstanceId);    
            }

            reader.NextResult();
        }

        return functionInstanceIds;
    }

    private string? _setFunctionStateSql;
    public async Task<bool> SetFunctionState(
        FlowId flowId, Status status, 
        string? param, string? result, 
        StoredException? storedException, 
        long? postponeUntil,
        int expectedEpoch)
    {
        await using var conn = await _connFunc();
    
        _setFunctionStateSql ??= @$"
            UPDATE {_tablePrefix}
            SET
                Status = @Status,
                ParamJson = @ParamJson,             
                ResultJson = @ResultJson,
                ExceptionJson = @ExceptionJson,
                PostponedUntil = @PostponedUntil,
                Epoch = Epoch + 1
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";
        
        await using var command = new SqlCommand(_setFunctionStateSql, conn);
        command.Parameters.AddWithValue("@Status", (int) status);
        command.Parameters.AddWithValue("@ParamJson", param == null ? DBNull.Value : param);
        command.Parameters.AddWithValue("@ResultJson", result == null ? DBNull.Value : result);
        var exceptionJson = storedException == null ? null : JsonSerializer.Serialize(storedException);
        command.Parameters.AddWithValue("@ExceptionJson", exceptionJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@PostponedUntil", postponeUntil ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
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
            UPDATE {_tablePrefix}
            SET Status = {(int) Status.Succeeded}, ResultJson = @ResultJson, DefaultStateJson = @DefaultStateJson, Timestamp = @Timestamp, Epoch = @ExpectedEpoch
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(_succeedFunctionSql, conn);
        command.Parameters.AddWithValue("@ResultJson", result == null ? DBNull.Value : result);
        command.Parameters.AddWithValue("@DefaultStateJson", defaultState ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
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
            UPDATE {_tablePrefix}
            SET Status = {(int) Status.Postponed}, PostponedUntil = @PostponedUntil, DefaultStateJson = @DefaultState, Timestamp = @Timestamp, Epoch = @ExpectedEpoch
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(_postponedFunctionSql, conn);
        command.Parameters.AddWithValue("@PostponedUntil", postponeUntil);
        command.Parameters.AddWithValue("@DefaultState", defaultState ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
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
            UPDATE {_tablePrefix}
            SET Status = {(int) Status.Failed}, ExceptionJson = @ExceptionJson, DefaultStateJson = @DefaultStateJson, Timestamp = @timestamp, Epoch = @ExpectedEpoch
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(_failFunctionSql, conn);
        command.Parameters.AddWithValue("@ExceptionJson", JsonSerializer.Serialize(storedException));
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@DefaultStateJson", defaultState ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
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
                UPDATE {_tablePrefix}
                SET Status = {(int)Status.Suspended}, DefaultStateJson = @DefaultStateJson, Timestamp = @Timestamp
                WHERE FunctionTypeId = @FunctionTypeId AND 
                      FunctionInstanceId = @FunctionInstanceId AND                       
                      Epoch = @ExpectedEpoch AND
                      InterruptCount = @ExpectedInterruptCount;";

        await using var command = new SqlCommand(_suspendFunctionSql, conn);
        command.Parameters.AddWithValue("@DefaultStateJson", defaultState ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);
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
                UPDATE {_tablePrefix}
                SET DefaultStateJson = @DefaultStateJson
                WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";

        await using var command = new SqlCommand(_setDefaultStateSql, conn);
        command.Parameters.AddWithValue("@DefaultStateJson", stateJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);

        await command.ExecuteNonQueryAsync();
    }

    private string? _setParametersSql;
    public async Task<bool> SetParameters(
        FlowId flowId,
        string? param, string? result,
        int expectedEpoch)
    {
        await using var conn = await _connFunc();
        
        _setParametersSql ??= @$"
            UPDATE {_tablePrefix}
            SET ParamJson = @ParamJson,  
                ResultJson = @ResultJson,
                Epoch = Epoch + 1
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(_setParametersSql, conn);
        command.Parameters.AddWithValue("@ParamJson", param == null ? DBNull.Value : param);
        command.Parameters.AddWithValue("@ResultJson", result == null ? DBNull.Value : result);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    private string? _incrementInterruptCountSql;
    public async Task<bool> IncrementInterruptCount(FlowId flowId)
    {
        await using var conn = await _connFunc();
        _incrementInterruptCountSql ??= @$"
                UPDATE {_tablePrefix}
                SET InterruptCount = InterruptCount + 1
                WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Status = {(int) Status.Executing};";

        await using var command = new SqlCommand(_incrementInterruptCountSql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _getInterruptCountSql;
    public async Task<long?> GetInterruptCount(FlowId flowId)
    {
        await using var conn = await _connFunc();
        _getInterruptCountSql ??= @$"
                SELECT InterruptCount 
                FROM {_tablePrefix}            
                WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId;";

        await using var command = new SqlCommand(_getInterruptCountSql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);

        var interruptCount = await command.ExecuteScalarAsync();
        return (long?) interruptCount;
    }

    private string? _getFunctionStatusSql;
    public async Task<StatusAndEpoch?> GetFunctionStatus(FlowId flowId)
    {
        await using var conn = await _connFunc();
        _getFunctionStatusSql ??= @$"
            SELECT Status, Epoch
            FROM {_tablePrefix}
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = new SqlCommand(_getFunctionStatusSql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);
        
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
    public async Task<StoredFunction?> GetFunction(FlowId flowId)
    {
        await using var conn = await _connFunc();
        _getFunctionSql ??= @$"
            SELECT  ParamJson, 
                    Status,
                    ResultJson, 
                    DefaultStateJson,
                    ExceptionJson,
                    PostponedUntil,
                    Epoch, 
                    LeaseExpiration,
                    InterruptCount,
                    Timestamp
            FROM {_tablePrefix}
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = new SqlCommand(_getFunctionSql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);
        
        await using var reader = await command.ExecuteReaderAsync();
        return ReadToStoredFunction(flowId, reader);
    }

    private StoredFunction? ReadToStoredFunction(FlowId flowId, SqlDataReader reader)
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
                var postponedUntil = reader.IsDBNull(5) ? default(long?) : reader.GetInt64(5);
                var epoch = reader.GetInt32(6);
                var leaseExpiration = reader.GetInt64(7);
                var interruptCount = reader.GetInt64(8);
                var timestamp = reader.GetInt64(9);

                return new StoredFunction(
                    flowId,
                    parameter,
                    defaultStateJson,
                    status,
                    result,
                    storedException,
                    postponedUntil,
                    epoch,
                    leaseExpiration,
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
            DELETE FROM {_tablePrefix}
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId ";
        
        await using var command = new SqlCommand(_deleteFunctionSql, conn);
        command.Parameters.AddWithValue("@FunctionInstanceId", flowId.Instance.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", flowId.Type.Value);
        
        return await command.ExecuteNonQueryAsync() == 1;
    }
}
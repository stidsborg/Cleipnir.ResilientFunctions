using System.Data;
using System.Text;
using System.Text.Json;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;
using static Cleipnir.ResilientFunctions.MySQL.DatabaseHelper;

namespace Cleipnir.ResilientFunctions.MySQL;

public class MySqlFunctionStore : IFunctionStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    private readonly MySqlMessageStore _messageStore;
    public IMessageStore MessageStore => _messageStore;
    
    private readonly MySqlEffectsStore _effectsStore;
    public IEffectsStore EffectsStore => _effectsStore;
    
    private readonly MySqlStatesStore _statesStore;
    public IStatesStore StatesStore => _statesStore;
    
    private readonly MySqlTimeoutStore _timeoutStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    private readonly MySqlCorrelationStore _correlationStore;
    public ICorrelationStore CorrelationStore => _correlationStore;

    public Utilities Utilities { get; }
    private readonly MySqlUnderlyingRegister _mySqlUnderlyingRegister;

    public MySqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        tablePrefix = tablePrefix == "" ? "rfunctions" : tablePrefix;
        
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
        _messageStore = new MySqlMessageStore(connectionString, tablePrefix);
        _effectsStore = new MySqlEffectsStore(connectionString, tablePrefix);
        _statesStore = new MySqlStatesStore(connectionString, tablePrefix);
        _correlationStore = new MySqlCorrelationStore(connectionString, tablePrefix);
        _timeoutStore = new MySqlTimeoutStore(connectionString, tablePrefix);
        _mySqlUnderlyingRegister = new(connectionString, _tablePrefix);
        Utilities = new Utilities(_mySqlUnderlyingRegister);
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await _mySqlUnderlyingRegister.Initialize();
        await MessageStore.Initialize();
        await EffectsStore.Initialize();
        await StatesStore.Initialize();
        await CorrelationStore.Initialize();
        await TimeoutStore.Initialize();
        await using var conn = await CreateOpenConnection(_connectionString);
        _initializeSql ??= $@"
            CREATE TABLE IF NOT EXISTS {_tablePrefix} (
                function_type_id VARCHAR(200) NOT NULL,
                function_instance_id VARCHAR(200) NOT NULL,
                param_json TEXT NULL,                    
                status INT NOT NULL,
                result_json TEXT NULL,
                default_state TEXT NULL DEFAULT NULL,
                exception_json TEXT NULL,
                postponed_until BIGINT NULL,
                epoch INT NOT NULL,
                lease_expiration BIGINT NOT NULL,
                interrupt_count BIGINT NOT NULL DEFAULT 0,
                timestamp BIGINT NOT NULL,
                PRIMARY KEY (function_type_id, function_instance_id),
                INDEX (function_type_id, status, function_instance_id)   
            );";

        await using var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _dropIfExistsSql;
    public async Task DropIfExists()
    {
        await _messageStore.DropUnderlyingTable();
        await _mySqlUnderlyingRegister.DropUnderlyingTable();
        await _timeoutStore.DropUnderlyingTable();

        await using var conn = await CreateOpenConnection(_connectionString);
        _dropIfExistsSql ??= $"DROP TABLE IF EXISTS {_tablePrefix}";
        await using var command = new MySqlCommand(_dropIfExistsSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateTablesSql;
    public async Task TruncateTables()
    {
        await _messageStore.TruncateTable();
        await _timeoutStore.Truncate();
        await _mySqlUnderlyingRegister.TruncateTable();
        await _effectsStore.Truncate();
        await _statesStore.Truncate();
        await _correlationStore.Truncate();
        
        await using var conn = await CreateOpenConnection(_connectionString);
        _truncateTablesSql ??= $"TRUNCATE TABLE {_tablePrefix}";
        await using var command = new MySqlCommand(_truncateTablesSql, conn);
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
        await using var conn = await CreateOpenConnection(_connectionString);

        var status = postponeUntil == null ? Status.Executing : Status.Postponed;
        _createFunctionSql ??= @$"
            INSERT IGNORE INTO {_tablePrefix}
                (function_type_id, function_instance_id, param_json, status, epoch, lease_expiration, postponed_until, timestamp)
            VALUES
                (?, ?, ?, ?, 0, ?, ?, ?)";
        await using var command = new MySqlCommand(_createFunctionSql, conn)
        {
            Parameters =
            {
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value},
                new() {Value = param ?? (object) DBNull.Value},
                new() {Value = (int) status}, 
                new() {Value = leaseExpiration},
                new() {Value = postponeUntil},
                new() {Value = timestamp}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task BulkScheduleFunctions(IEnumerable<FunctionIdWithParam> functionsWithParam)
    {
        var insertSql = @$"
            INSERT IGNORE INTO {_tablePrefix}
              (function_type_id, function_instance_id, param_json, status, epoch, lease_expiration, postponed_until, timestamp)
            VALUES                      
                    ";
        
        var now = DateTime.UtcNow.Ticks;
     
        var rows = new List<string>();
        foreach (var ((type, instance), param) in functionsWithParam)
        {
            var row = $"('{type.Value.EscapeString()}', '{instance.Value.EscapeString()}', {(param == null ? "NULL" : $"'{param.EscapeString()}'")}, {(int) Status.Postponed}, 0, 0, 0, {now})";
            rows.Add(row);
        }
        var rowsSql = string.Join(", " + Environment.NewLine, rows);
        var strBuilder = new StringBuilder(rowsSql.Length + 2);
        strBuilder.Append(insertSql);
        strBuilder.Append(rowsSql);
        strBuilder.Append(";");
        var sql = strBuilder.ToString();

        await using var conn = await CreateOpenConnection(_connectionString);
        await using var cmd = new MySqlCommand(sql, conn);
        cmd.ExecuteNonQuery();
    }

    private string? _restartExecutionSql;
    public async Task<StoredFunction?> RestartExecution(FlowId flowId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _restartExecutionSql ??= @$"
            UPDATE {_tablePrefix}
            SET epoch = epoch + 1, status = {(int)Status.Executing}, lease_expiration = ?
            WHERE function_type_id = ? AND function_instance_id = ? AND epoch = ?;
            SELECT               
                param_json,            
                status,
                result_json, 
                default_state,
                exception_json,
                postponed_until,
                epoch, 
                lease_expiration,
                interrupt_count,
                timestamp
            FROM {_tablePrefix}
            WHERE function_type_id = ? AND function_instance_id = ?;";

        await using var command = new MySqlCommand(_restartExecutionSql, conn)
        {
            Parameters =
            {
                new() { Value = leaseExpiration },
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
                new() { Value = expectedEpoch },
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
            }
        };

        var reader = await command.ExecuteReaderAsync();
        if (reader.RecordsAffected == 0)
            return default;

        var sf = await ReadToStoredFunction(flowId, reader);
        return sf?.Epoch == expectedEpoch + 1
            ? sf
            : default;
    }

    private string? _renewLeaseSql;
    public async Task<bool> RenewLease(FlowId flowId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _renewLeaseSql ??= $@"
            UPDATE {_tablePrefix}
            SET lease_expiration = ?
            WHERE function_type_id = ? AND function_instance_id = ? AND epoch = ? AND status = {(int) Status.Executing}";
        await using var command = new MySqlCommand(_renewLeaseSql, conn)
        {
            Parameters =
            {
                new() {Value = leaseExpiration},
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _getCrashedFunctionsSql;
    public async Task<IReadOnlyList<InstanceIdAndEpoch>> GetCrashedFunctions(FlowType flowType, long leaseExpiresBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getCrashedFunctionsSql ??= @$"
            SELECT function_instance_id, epoch 
            FROM {_tablePrefix}
            WHERE function_type_id = ? AND lease_expiration < ? AND status = {(int) Status.Executing}";
        await using var command = new MySqlCommand(_getCrashedFunctionsSql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value},
                new() { Value = leaseExpiresBefore }
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<InstanceIdAndEpoch>();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            var epoch = reader.GetInt32(1);
            functions.Add(new InstanceIdAndEpoch(functionInstanceId, epoch));
        }
        
        return functions;
    }

    private string? _getPostponedFunctionsSql;
    public async Task<IReadOnlyList<InstanceIdAndEpoch>> GetPostponedFunctions(FlowType flowType, long isEligibleBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getPostponedFunctionsSql ??= @$"
            SELECT function_instance_id, epoch
            FROM {_tablePrefix}
            WHERE function_type_id = ? AND status = {(int) Status.Postponed} AND postponed_until <= ?";
        await using var command = new MySqlCommand(_getPostponedFunctionsSql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value},
                new() {Value = isEligibleBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var functions = new List<InstanceIdAndEpoch>();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            var epoch = reader.GetInt32(1);
            functions.Add(new InstanceIdAndEpoch(functionInstanceId, epoch));
        }
        
        return functions;
    }

    private string? _getSucceededFunctionsSql;
    public async Task<IReadOnlyList<FlowInstance>> GetSucceededFunctions(FlowType flowType, long completedBefore)
    {
        
        await using var conn = await CreateOpenConnection(_connectionString);
        _getSucceededFunctionsSql ??= @$"
            SELECT function_instance_id
            FROM {_tablePrefix}
            WHERE function_type_id = ? AND status = {(int) Status.Succeeded} AND timestamp <= ?";
        await using var command = new MySqlCommand(_getSucceededFunctionsSql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value},
                new() {Value = completedBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var functions = new List<FlowInstance>();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            functions.Add(functionInstanceId);
        }
        
        return functions;
    }

    private string? _setFunctionStateSql;
    public async Task<bool> SetFunctionState(
        FlowId flowId, Status status, 
        string? storedParameter, string? storedResult, 
        StoredException? storedException, 
        long? postponeUntil,
        int expectedEpoch)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
      
        _setFunctionStateSql ??= $@"
            UPDATE {_tablePrefix}
            SET status = ?, 
                param_json = ?,  
                result_json = ?,  
                exception_json = ?, postponed_until = ?,
                epoch = epoch + 1
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";
        await using var command = new MySqlCommand(_setFunctionStateSql, conn)
        {
            Parameters =
            {
                new() {Value = (int) status},
                new() {Value = storedParameter ?? (object) DBNull.Value},
                new() {Value = storedResult ?? (object) DBNull.Value},
                new() {Value = storedException != null ? JsonSerializer.Serialize(storedException) : DBNull.Value},
                new() {Value = postponeUntil ?? (object) DBNull.Value},
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
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
        await using var conn = await CreateOpenConnection(_connectionString);
        _succeedFunctionSql ??= $@"
            UPDATE {_tablePrefix}
            SET status = {(int) Status.Succeeded}, result_json = ?, default_state = ?, timestamp = ?, epoch = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(_succeedFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = result ?? (object)DBNull.Value },
                new() { Value = defaultState ?? (object)DBNull.Value },
                new() { Value = timestamp },
                new() { Value = expectedEpoch },
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
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
        await using var conn = await CreateOpenConnection(_connectionString);
        _postponedFunctionSql ??= $@"
            UPDATE {_tablePrefix}
            SET status = {(int) Status.Postponed}, postponed_until = ?, default_state = ?, timestamp = ?, epoch = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(_postponedFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = postponeUntil },
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = expectedEpoch },
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
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
        await using var conn = await CreateOpenConnection(_connectionString);
        _failFunctionSql ??= $@"
            UPDATE {_tablePrefix}
            SET status = {(int) Status.Failed}, exception_json = ?, default_state = ?, timestamp = ?, epoch = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(_failFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = JsonSerializer.Serialize(storedException) },
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = expectedEpoch },
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
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
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _suspendFunctionSql ??= $@"
            UPDATE {_tablePrefix}
            SET status = {(int) Status.Suspended}, default_state = ?, timestamp = ?
            WHERE function_type_id = ? AND 
                  function_instance_id = ? AND 
                  epoch = ? AND
                  interrupt_count = ?;";

        await using var command = new MySqlCommand(_suspendFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
                new() { Value = expectedEpoch },
                new() { Value = expectedInterruptCount }
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _setDefaultStateSql;
    public async Task SetDefaultState(FlowId flowId, string? stateJson)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _setDefaultStateSql ??= $@"
            UPDATE {_tablePrefix}
            SET default_state = ?
            WHERE function_type_id = ? AND function_instance_id = ?";
        await using var command = new MySqlCommand(_setDefaultStateSql, conn)
        {
            Parameters =
            {
                new() {Value = stateJson ?? (object) DBNull.Value},
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _setParametersSql;
    public async Task<bool> SetParameters(
        FlowId flowId,
        string? storedParameter, string? storedResult,
        int expectedEpoch)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
      
        _setParametersSql ??= $@"
            UPDATE {_tablePrefix}
            SET param_json = ?,  
                result_json = ?,
                epoch = epoch + 1
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(_setParametersSql, conn)
        {
            Parameters =
            {
                new() { Value = storedParameter ?? (object) DBNull.Value },
                new() { Value = storedResult ?? (object) DBNull.Value },
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
                new() { Value = expectedEpoch },
            }
        };
            
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _incrementInterruptCountSql;
    public async Task<bool> IncrementInterruptCount(FlowId flowId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _incrementInterruptCountSql ??= $@"
            UPDATE {_tablePrefix}
            SET interrupt_count = interrupt_count + 1
            WHERE function_type_id = ? AND function_instance_id = ? AND status = {(int) Status.Executing};";

        await using var command = new MySqlCommand(_incrementInterruptCountSql, conn)
        {
            Parameters =
            {
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _getInterruptCountSql;
    public async Task<long?> GetInterruptCount(FlowId flowId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _getInterruptCountSql ??= $@"
            SELECT interrupt_count 
            FROM {_tablePrefix}
            WHERE function_type_id = ? AND function_instance_id = ?;";

        await using var command = new MySqlCommand(_getInterruptCountSql, conn)
        {
            Parameters =
            {
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
            }
        };
        
        return (long?) await command.ExecuteScalarAsync();
    }

    private string? _getFunctionStatusSql;
    public async Task<StatusAndEpoch?> GetFunctionStatus(FlowId flowId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getFunctionStatusSql ??= $@"
            SELECT status, epoch
            FROM {_tablePrefix}
            WHERE function_type_id = ? AND function_instance_id = ?;";
        await using var command = new MySqlCommand(_getFunctionStatusSql, conn)
        {
            Parameters = { 
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            return new StatusAndEpoch(
                Status: (Status) reader.GetInt32(0),
                Epoch: reader.GetInt32(1)
            );
        }

        return null;
    }

    private string? _getFunctionSql;
    public async Task<StoredFunction?> GetFunction(FlowId flowId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getFunctionSql ??= $@"
            SELECT               
                param_json,             
                status,
                result_json,             
                default_state,
                exception_json,
                postponed_until,
                epoch, 
                lease_expiration,
                interrupt_count,
                timestamp
            FROM {_tablePrefix}
            WHERE function_type_id = ? AND function_instance_id = ?;";
        await using var command = new MySqlCommand(_getFunctionSql, conn)
        {
            Parameters = { 
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        return await ReadToStoredFunction(flowId, reader);
    }

    private async Task<StoredFunction?> ReadToStoredFunction(FlowId flowId, MySqlDataReader reader)
    {
        const int paramIndex = 0;
        const int statusIndex = 1;
        const int resultIndex = 2;
        const int defaultStateIndex = 3;
        const int exceptionIndex = 4;
        const int postponeUntilIndex = 5;
        const int epochIndex = 6;
        const int leaseExpirationIndex = 7;
        const int interruptCountIndex = 8;
        const int timestampIndex = 9;
        
        while (await reader.ReadAsync())
        {
            var hasParam = !await reader.IsDBNullAsync(paramIndex);
            var hasResult = !await reader.IsDBNullAsync(resultIndex);
            var hasDefaultState = !await reader.IsDBNullAsync(defaultStateIndex);
            var defaultState = hasDefaultState
                ? reader.GetString(defaultStateIndex)
                : null;
            var hasError = !await reader.IsDBNullAsync(exceptionIndex);
            var storedException = hasError
                ? JsonSerializer.Deserialize<StoredException>(reader.GetString(exceptionIndex))
                : null;
            var postponedUntil = !await reader.IsDBNullAsync(postponeUntilIndex);
            return new StoredFunction(
                flowId,
                hasParam ? reader.GetString(paramIndex) : null,
                defaultState,
                Status: (Status) reader.GetInt32(statusIndex),
                Result: hasResult ? reader.GetString(resultIndex) : null, 
                storedException,
                postponedUntil ? reader.GetInt64(postponeUntilIndex) : null,
                Epoch: reader.GetInt32(epochIndex),
                LeaseExpiration: reader.GetInt64(leaseExpirationIndex),
                InterruptCount: reader.GetInt64(interruptCountIndex),
                Timestamp: reader.GetInt64(timestampIndex)
            );
        }

        return null;
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
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _deleteFunctionSql ??= $@"            
            DELETE FROM {_tablePrefix}
            WHERE function_type_id = ? AND function_instance_id = ?";
        
        await using var command = new MySqlCommand(_deleteFunctionSql, conn)
        {
            Parameters =
            {
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value}
            }
        };

        return await command.ExecuteNonQueryAsync() == 1;
    }
}
using System.Text;
using System.Text.Json;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
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
    
    private readonly MySqlTimeoutStore _timeoutStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    private readonly MySqlCorrelationStore _correlationStore;
    public ICorrelationStore CorrelationStore => _correlationStore;

    public IMigrator Migrator => _migrator;
    private readonly MySqlMigrator _migrator;

    public Utilities Utilities { get; }
    private readonly MySqlUnderlyingRegister _mySqlUnderlyingRegister;

    public MySqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        tablePrefix = tablePrefix == "" ? "rfunctions" : tablePrefix;
        
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
        
        _messageStore = new MySqlMessageStore(connectionString, tablePrefix);
        _effectsStore = new MySqlEffectsStore(connectionString, tablePrefix);
        _correlationStore = new MySqlCorrelationStore(connectionString, tablePrefix);
        _timeoutStore = new MySqlTimeoutStore(connectionString, tablePrefix);
        _mySqlUnderlyingRegister = new MySqlUnderlyingRegister(connectionString, tablePrefix);
        _migrator  = new MySqlMigrator(connectionString, tablePrefix);
        
        Utilities = new Utilities(_mySqlUnderlyingRegister);
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        var createTables = await _migrator.InitializeAndMigrate();
        if (!createTables)
            return;
        
        await _mySqlUnderlyingRegister.Initialize();
        await MessageStore.Initialize();
        await EffectsStore.Initialize();
        await CorrelationStore.Initialize();
        await TimeoutStore.Initialize();
        await using var conn = await CreateOpenConnection(_connectionString);
        _initializeSql ??= $@"
            CREATE TABLE IF NOT EXISTS {_tablePrefix} (
                type VARCHAR(200) NOT NULL,
                instance VARCHAR(200) NOT NULL,
                epoch INT NOT NULL,
                status INT NOT NULL,
                expires BIGINT NOT NULL,
                interrupt_count BIGINT NOT NULL DEFAULT 0,                
                param_json TEXT NULL,                                    
                result_json TEXT NULL,
                default_state TEXT NULL DEFAULT NULL,
                exception_json TEXT NULL,                
                timestamp BIGINT NOT NULL,
                PRIMARY KEY (type, instance),
                INDEX (expires, type, instance, status)   
            );";

        await using var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateTablesSql;
    public async Task TruncateTables()
    {
        await _messageStore.TruncateTable();
        await _timeoutStore.Truncate();
        await _mySqlUnderlyingRegister.TruncateTable();
        await _effectsStore.Truncate();
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
                (type, instance, param_json, status, epoch, expires, timestamp)
            VALUES
                (?, ?, ?, ?, 0, ?, ?)";
        await using var command = new MySqlCommand(_createFunctionSql, conn)
        {
            Parameters =
            {
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value},
                new() {Value = param ?? (object) DBNull.Value},
                new() {Value = (int) status}, 
                new() {Value = postponeUntil ?? leaseExpiration},
                new() {Value = timestamp}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam)
    {
        var insertSql = @$"
            INSERT IGNORE INTO {_tablePrefix}
              (type, instance, param_json, status, epoch, expires, timestamp)
            VALUES                      
                    ";
        
        var now = DateTime.UtcNow.Ticks;
     
        var rows = new List<string>();
        foreach (var ((type, instance), param) in functionsWithParam)
        {
            var row = $"('{type.Value.EscapeString()}', '{instance.Value.EscapeString()}', {(param == null ? "NULL" : $"'{param.EscapeString()}'")}, {(int) Status.Postponed}, 0, 0, {now})";
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
    public async Task<StoredFlow?> RestartExecution(FlowId flowId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _restartExecutionSql ??= @$"
            UPDATE {_tablePrefix}
            SET epoch = epoch + 1, status = {(int)Status.Executing}, expires = ?
            WHERE type = ? AND instance = ? AND epoch = ?;
            SELECT               
                param_json,            
                status,
                result_json, 
                default_state,
                exception_json,
                epoch, 
                expires,
                interrupt_count,
                timestamp
            FROM {_tablePrefix}
            WHERE type = ? AND instance = ?;";

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
            SET expires = ?
            WHERE type = ? AND instance = ? AND epoch = ? AND status = {(int) Status.Executing}";
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

    private string? _getExpiredFunctionsSql;
    public async Task<IReadOnlyList<IdAndEpoch>> GetExpiredFunctions(long expiredBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getExpiredFunctionsSql ??= @$"
            SELECT type, instance, epoch
            FROM {_tablePrefix}
            WHERE expires <= ? AND (status = {(int) Status.Executing} OR status = {(int) Status.Postponed})";
        await using var command = new MySqlCommand(_getExpiredFunctionsSql, conn)
        {
            Parameters =
            {
                new() {Value = expiredBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var functions = new List<IdAndEpoch>();
        while (await reader.ReadAsync())
        {
            var flowType = reader.GetString(0);
            var flowInstance = reader.GetString(1);
            var flowId = new FlowId(flowType, flowInstance);
            var epoch = reader.GetInt32(2);
            functions.Add(new IdAndEpoch(flowId, epoch));
        }
        
        return functions;
    }

    private string? _getSucceededFunctionsSql;
    public async Task<IReadOnlyList<FlowInstance>> GetSucceededFunctions(FlowType flowType, long completedBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getSucceededFunctionsSql ??= @$"
            SELECT instance
            FROM {_tablePrefix}
            WHERE type = ? AND status = {(int) Status.Succeeded} AND timestamp <= ?";
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
            var flowInstance = reader.GetString(0);
            functions.Add(flowInstance);
        }
        
        return functions;
    }

    private string? _setFunctionStateSql;
    public async Task<bool> SetFunctionState(
        FlowId flowId, Status status, 
        string? storedParameter, string? storedResult, 
        StoredException? storedException, 
        long expires,
        int expectedEpoch)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
      
        _setFunctionStateSql ??= $@"
            UPDATE {_tablePrefix}
            SET status = ?, 
                param_json = ?,  
                result_json = ?,  
                exception_json = ?, expires = ?,
                epoch = epoch + 1
            WHERE 
                type = ? AND 
                instance = ? AND 
                epoch = ?";
        await using var command = new MySqlCommand(_setFunctionStateSql, conn)
        {
            Parameters =
            {
                new() {Value = (int) status},
                new() {Value = storedParameter ?? (object) DBNull.Value},
                new() {Value = storedResult ?? (object) DBNull.Value},
                new() {Value = storedException != null ? JsonSerializer.Serialize(storedException) : DBNull.Value},
                new() {Value = expires},
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
                type = ? AND 
                instance = ? AND 
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
            SET status = {(int) Status.Postponed}, expires = ?, default_state = ?, timestamp = ?, epoch = ?
            WHERE 
                type = ? AND 
                instance = ? AND 
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
                type = ? AND 
                instance = ? AND 
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
            WHERE type = ? AND 
                  instance = ? AND 
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
            WHERE type = ? AND instance = ?";
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

    public async Task<bool> Interrupt(FlowId flowId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        var sql = $@"
            UPDATE {_tablePrefix}
            SET 
                interrupt_count = interrupt_count + 1,
                status = 
                    CASE 
                        WHEN status = {(int) Status.Suspended} THEN {(int) Status.Postponed}
                        ELSE status
                    END,
                expires = 
                    CASE
                        WHEN status = {(int) Status.Postponed} THEN 0
                        WHEN status = {(int) Status.Suspended} THEN 0
                        ELSE expires
                    END
            WHERE type = ? AND instance = ?;";

        await using var command = new MySqlCommand(sql, conn)
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
                type = ? AND 
                instance = ? AND 
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
            WHERE type = ? AND instance = ? AND status = {(int) Status.Executing};";

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
            WHERE type = ? AND instance = ?;";

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
            WHERE type = ? AND instance = ?;";
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
    public async Task<StoredFlow?> GetFunction(FlowId flowId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getFunctionSql ??= $@"
            SELECT               
                param_json,             
                status,
                result_json,             
                default_state,
                exception_json,               
                epoch, 
                expires,
                interrupt_count,
                timestamp
            FROM {_tablePrefix}
            WHERE type = ? AND instance = ?;";
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

    private string? _getInstancesWithStatusSql;
    public async Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType, Status status)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getInstancesWithStatusSql ??= @$"
            SELECT instance
            FROM {_tablePrefix}
            WHERE type = ? AND status = ?";
        await using var command = new MySqlCommand(_getInstancesWithStatusSql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value},
                new() {Value = (int) status}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var instances = new List<FlowInstance>();
        while (await reader.ReadAsync())
        {
            var flowInstance = reader.GetString(0);
            instances.Add(flowInstance);
        }
        
        return instances;
    }

    private string? _getInstancesSql;
    public async Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getInstancesSql ??= @$"
            SELECT instance
            FROM {_tablePrefix}
            WHERE type = ?";
        await using var command = new MySqlCommand(_getInstancesSql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var functions = new List<FlowInstance>();
        while (await reader.ReadAsync())
        {
            var flowInstance = reader.GetString(0);
            functions.Add(flowInstance);
        }
        
        return functions;
    }

    private string? _getTypesSql;
    public async Task<IReadOnlyList<FlowType>> GetTypes()
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getTypesSql ??= $"SELECT DISTINCT(type) FROM {_tablePrefix}";
        await using var command = new MySqlCommand(_getTypesSql, conn);
        
        await using var reader = await command.ExecuteReaderAsync();
        var flowTypes = new List<FlowType>();
        while (await reader.ReadAsync())
        {
            var flowType = reader.GetString(0);
            flowTypes.Add(flowType);
        }
        
        return flowTypes;
    }

    private async Task<StoredFlow?> ReadToStoredFunction(FlowId flowId, MySqlDataReader reader)
    {
        const int paramIndex = 0;
        const int statusIndex = 1;
        const int resultIndex = 2;
        const int defaultStateIndex = 3;
        const int exceptionIndex = 4;
        const int epochIndex = 5;
        const int expiresIndex = 6;
        const int interruptCountIndex = 7;
        const int timestampIndex = 8;
        
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
            return new StoredFlow(
                flowId,
                hasParam ? reader.GetString(paramIndex) : null,
                defaultState,
                Status: (Status) reader.GetInt32(statusIndex),
                Result: hasResult ? reader.GetString(resultIndex) : null, 
                storedException, Epoch: reader.GetInt32(epochIndex),
                Expires: reader.GetInt64(expiresIndex),
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
            WHERE type = ? AND instance = ?";
        
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
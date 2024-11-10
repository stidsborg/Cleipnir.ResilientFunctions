using System.Text;
using System.Text.Json;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;
using static Cleipnir.ResilientFunctions.MariaDb.DatabaseHelper;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbFunctionStore : IFunctionStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    private readonly MariaDbMessageStore _messageStore;
    public IMessageStore MessageStore => _messageStore;
    
    private readonly MariaDbEffectsStore _effectsStore;
    public IEffectsStore EffectsStore => _effectsStore;
    
    private readonly MariaDbTimeoutStore _timeoutStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    
    private readonly MariaDbTypeStore _typeStore;
    public ITypeStore TypeStore => _typeStore;
    
    private readonly MariaDbCorrelationStore _correlationStore;
    public ICorrelationStore CorrelationStore => _correlationStore;

    public IMigrator Migrator => _migrator;
    public ILogStore LogStore => throw new NotImplementedException();
    private readonly MariaDbMigrator _migrator;

    public Utilities Utilities { get; }
    private readonly MariaDbUnderlyingRegister _mariaDbUnderlyingRegister;

    public MariaDbFunctionStore(string connectionString, string tablePrefix = "")
    {
        tablePrefix = tablePrefix == "" ? "rfunctions" : tablePrefix;
        
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
        
        _messageStore = new MariaDbMessageStore(connectionString, tablePrefix);
        _effectsStore = new MariaDbEffectsStore(connectionString, tablePrefix);
        _correlationStore = new MariaDbCorrelationStore(connectionString, tablePrefix);
        _timeoutStore = new MariaDbTimeoutStore(connectionString, tablePrefix);
        _mariaDbUnderlyingRegister = new MariaDbUnderlyingRegister(connectionString, tablePrefix);
        _typeStore = new MariaDbTypeStore(connectionString, tablePrefix);
        _migrator  = new MariaDbMigrator(connectionString, tablePrefix);
        
        Utilities = new Utilities(_mariaDbUnderlyingRegister);
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        var createTables = await _migrator.InitializeAndMigrate();
        if (!createTables)
            return;
        
        await _mariaDbUnderlyingRegister.Initialize();
        await MessageStore.Initialize();
        await EffectsStore.Initialize();
        await CorrelationStore.Initialize();
        await TimeoutStore.Initialize();
        await _typeStore.Initialize();
        await using var conn = await CreateOpenConnection(_connectionString);
        _initializeSql ??= $@"
            CREATE TABLE IF NOT EXISTS {_tablePrefix} (
                type INT NOT NULL,
                instance CHAR(32) NOT NULL,
                epoch INT NOT NULL,
                status INT NOT NULL,
                expires BIGINT NOT NULL,
                interrupted BOOLEAN NOT NULL,                
                param_json LONGBLOB NULL,                                    
                result_json LONGBLOB NULL,
                exception_json TEXT NULL,                
                timestamp BIGINT NOT NULL,
                human_instance_id TEXT NOT NULL,
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
        await _mariaDbUnderlyingRegister.TruncateTable();
        await _effectsStore.Truncate();
        await _correlationStore.Truncate();
        await _typeStore.Truncate();
        
        await using var conn = await CreateOpenConnection(_connectionString);
        _truncateTablesSql ??= $"TRUNCATE TABLE {_tablePrefix}";
        await using var command = new MySqlCommand(_truncateTablesSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _createFunctionSql;
    public async Task<bool> CreateFunction(
        StoredId storedId, 
        string humanInstanceId,
        byte[]? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp)
    {
        await using var conn = await CreateOpenConnection(_connectionString);

        var status = postponeUntil == null ? Status.Executing : Status.Postponed;
        _createFunctionSql ??= @$"
            INSERT IGNORE INTO {_tablePrefix}
                (type, instance, param_json, status, epoch, expires, timestamp, human_instance_id)
            VALUES
                (?, ?, ?, ?, 0, ?, ?, ?)";
        await using var command = new MySqlCommand(_createFunctionSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value.ToString("N")},
                new() {Value = param ?? (object) DBNull.Value},
                new() {Value = (int) status}, 
                new() {Value = postponeUntil ?? leaseExpiration},
                new() {Value = timestamp},
                new() {Value = humanInstanceId}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam)
    {
        var insertSql = @$"
            INSERT IGNORE INTO {_tablePrefix}
              (type, instance, param_json, status, epoch, expires, timestamp, human_instance_id)
            VALUES                      
                    ";
        
        var now = DateTime.UtcNow.Ticks;
     
        var rows = new List<string>();
        foreach (var ((type, instance), humanInstanceId, param) in functionsWithParam)
        {
            var row = $"({type.Value}, '{instance.Value:N}', {(param == null ? "NULL" : $"x'{Convert.ToHexString(param)}'")}, {(int) Status.Postponed}, 0, 0, {now}, '{humanInstanceId.EscapeString()}')"; 
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
    public async Task<StoredFlow?> RestartExecution(StoredId storedId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _restartExecutionSql ??= @$"
            UPDATE {_tablePrefix}
            SET epoch = epoch + 1, status = {(int)Status.Executing}, expires = ?, interrupted = FALSE
            WHERE type = ? AND instance = ? AND epoch = ?;
            SELECT               
                param_json,            
                status,
                result_json, 
                exception_json,
                epoch, 
                expires,
                interrupted,
                timestamp,
                human_instance_id
            FROM {_tablePrefix}
            WHERE type = ? AND instance = ?;";

        await using var command = new MySqlCommand(_restartExecutionSql, conn)
        {
            Parameters =
            {
                new() { Value = leaseExpiration },
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value.ToString("N") },
                new() { Value = expectedEpoch },
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value.ToString("N") },
            }
        };

        var reader = await command.ExecuteReaderAsync();
        if (reader.RecordsAffected == 0)
            return default;

        var sf = await ReadToStoredFunction(storedId, reader);
        return sf?.Epoch == expectedEpoch + 1
            ? sf
            : default;
    }

    private string? _renewLeaseSql;
    public async Task<bool> RenewLease(StoredId storedId, int expectedEpoch, long leaseExpiration)
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
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value.ToString("N")},
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
            var flowType = reader.GetInt32(0);
            var flowInstance = reader.GetString(1).ToGuid().ToStoredInstance();
            var flowId = new StoredId(new StoredType(flowType), flowInstance);
            var epoch = reader.GetInt32(2);
            functions.Add(new IdAndEpoch(flowId, epoch));
        }
        
        return functions;
    }

    private string? _getSucceededFunctionsSql;
    public async Task<IReadOnlyList<StoredInstance>> GetSucceededFunctions(StoredType storedType, long completedBefore)
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
                new() {Value = storedType.Value},
                new() {Value = completedBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var instances = new List<StoredInstance>();
        while (await reader.ReadAsync())
        {
            var instance = reader.GetString(0).ToGuid().ToStoredInstance();
            instances.Add(instance);
        }
        
        return instances;
    }

    private string? _setFunctionStateSql;
    public async Task<bool> SetFunctionState(
        StoredId storedId, Status status, 
        byte[]? storedParameter, byte[]? storedResult, 
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
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value.ToString("N")},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _succeedFunctionSql;
    public async Task<bool> SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _succeedFunctionSql ??= $@"
            UPDATE {_tablePrefix}
            SET status = {(int) Status.Succeeded}, result_json = ?, timestamp = ?, epoch = ?
            WHERE 
                type = ? AND 
                instance = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(_succeedFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = result ?? (object)DBNull.Value },
                new() { Value = timestamp },
                new() { Value = expectedEpoch },
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value.ToString("N") },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _postponedFunctionSql;
    public async Task<bool> PostponeFunction(
        StoredId storedId, 
        long postponeUntil, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _postponedFunctionSql ??= $@"
            UPDATE {_tablePrefix}
            SET status = {(int) Status.Postponed}, expires = ?, timestamp = ?, epoch = ?
            WHERE 
                type = ? AND 
                instance = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(_postponedFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = postponeUntil },
                new() { Value = timestamp },
                new() { Value = expectedEpoch },
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value.ToString("N") },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _failFunctionSql;
    public async Task<bool> FailFunction(
        StoredId storedId, 
        StoredException storedException, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _failFunctionSql ??= $@"
            UPDATE {_tablePrefix}
            SET status = {(int) Status.Failed}, exception_json = ?, timestamp = ?, epoch = ?
            WHERE 
                type = ? AND 
                instance = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(_failFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = JsonSerializer.Serialize(storedException) },
                new() { Value = timestamp },
                new() { Value = expectedEpoch },
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value.ToString("N") },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _suspendFunctionSql;
    public async Task<bool> SuspendFunction(
        StoredId storedId, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _suspendFunctionSql ??= $@"
            UPDATE {_tablePrefix}
            SET status = {(int) Status.Suspended}, timestamp = ?
            WHERE type = ? AND 
                  instance = ? AND 
                  epoch = ? AND
                  NOT interrupted";

        await using var command = new MySqlCommand(_suspendFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = timestamp },
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value.ToString("N") },
                new() { Value = expectedEpoch }
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _interruptSql;
    private string? _interruptIfExecutingSql;
    public async Task<bool> Interrupt(StoredId storedId, bool onlyIfExecuting)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _interruptSql ??= $@"
            UPDATE {_tablePrefix}
            SET 
                interrupted = TRUE,
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
            WHERE type = ? AND instance = ?";
        _interruptIfExecutingSql ??= _interruptSql + $" AND status = {(int) Status.Executing}";

        var sql = onlyIfExecuting
            ? _interruptIfExecutingSql
            : _interruptSql;
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value.ToString("N") },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _setParametersSql;
    public async Task<bool> SetParameters(
        StoredId storedId,
        byte[]? storedParameter, byte[]? storedResult,
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
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value.ToString("N") },
                new() { Value = expectedEpoch },
            }
        };
            
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _getInterruptCountSql;
    public async Task<bool?> Interrupted(StoredId storedId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _getInterruptCountSql ??= $@"
            SELECT interrupted 
            FROM {_tablePrefix}
            WHERE type = ? AND instance = ?;";

        await using var command = new MySqlCommand(_getInterruptCountSql, conn)
        {
            Parameters =
            {
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value.ToString("N") },
            }
        };
        
        return (bool?) await command.ExecuteScalarAsync();
    }

    private string? _getFunctionStatusSql;
    public async Task<StatusAndEpoch?> GetFunctionStatus(StoredId storedId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getFunctionStatusSql ??= $@"
            SELECT status, epoch
            FROM {_tablePrefix}
            WHERE type = ? AND instance = ?;";
        await using var command = new MySqlCommand(_getFunctionStatusSql, conn)
        {
            Parameters = { 
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value.ToString("N")}
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
    public async Task<StoredFlow?> GetFunction(StoredId storedId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getFunctionSql ??= $@"
            SELECT               
                param_json,             
                status,
                result_json,             
                exception_json,               
                epoch, 
                expires,
                interrupted,
                timestamp,
                human_instance_id
            FROM {_tablePrefix}
            WHERE type = ? AND instance = ?;";
        await using var command = new MySqlCommand(_getFunctionSql, conn)
        {
            Parameters = { 
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value.ToString("N")}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        return await ReadToStoredFunction(storedId, reader);
    }

    private string? _getInstancesWithStatusSql;
    public async Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType, Status status)
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
                new() {Value = storedType.Value},
                new() {Value = (int) status}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var instances = new List<StoredInstance>();
        while (await reader.ReadAsync())
        {
            var flowInstance = reader.GetString(0).ToGuid().ToStoredInstance();
            instances.Add(flowInstance);
        }
        
        return instances;
    }

    private string? _getInstancesSql;
    public async Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType)
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
                new() {Value = storedType.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var functions = new List<StoredInstance>();
        while (await reader.ReadAsync())
        {
            var flowInstance = reader.GetString(0).ToGuid().ToStoredInstance();
            functions.Add(flowInstance);
        }
        
        return functions;
    }
    
    private async Task<StoredFlow?> ReadToStoredFunction(StoredId storedId, MySqlDataReader reader)
    {
        const int paramIndex = 0;
        const int statusIndex = 1;
        const int resultIndex = 2;
        const int exceptionIndex = 3;
        const int epochIndex = 4;
        const int expiresIndex = 5;
        const int interruptedIndex = 6;
        const int timestampIndex = 7;
        const int humanInstanceIdIndex = 8;
        
        while (await reader.ReadAsync())
        {
            var hasParam = !await reader.IsDBNullAsync(paramIndex);
            var hasResult = !await reader.IsDBNullAsync(resultIndex);
            var hasError = !await reader.IsDBNullAsync(exceptionIndex);
            var storedException = hasError
                ? JsonSerializer.Deserialize<StoredException>(reader.GetString(exceptionIndex))
                : null;
            return new StoredFlow(
                storedId,
                HumanInstanceId: reader.GetString(humanInstanceIdIndex),
                hasParam ? (byte[]) reader.GetValue(paramIndex) : null,
                Status: (Status) reader.GetInt32(statusIndex),
                Result: hasResult ? (byte[]) reader.GetValue(resultIndex) : null, 
                storedException, Epoch: reader.GetInt32(epochIndex),
                Expires: reader.GetInt64(expiresIndex),
                Interrupted: reader.GetBoolean(interruptedIndex),
                Timestamp: reader.GetInt64(timestampIndex)
            );
        }

        return null;
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
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _deleteFunctionSql ??= $@"            
            DELETE FROM {_tablePrefix}
            WHERE type = ? AND instance = ?";
        
        await using var command = new MySqlCommand(_deleteFunctionSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value.ToString("N")}
            }
        };

        return await command.ExecuteNonQueryAsync() == 1;
    }
}
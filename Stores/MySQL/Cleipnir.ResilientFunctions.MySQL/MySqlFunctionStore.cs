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
    
    private readonly MySqlStatesStore _statesStore;
    public IStatesStore StatesStore => _statesStore;
    
    private readonly MySqlTimeoutStore _timeoutStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    
    public Utilities Utilities { get; }
    private readonly MySqlUnderlyingRegister _mySqlUnderlyingRegister;

    public MySqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
        _messageStore = new MySqlMessageStore(connectionString, tablePrefix);
        _effectsStore = new MySqlEffectsStore(connectionString, tablePrefix);
        _statesStore = new MySqlStatesStore(connectionString, tablePrefix);
        _timeoutStore = new MySqlTimeoutStore(connectionString, tablePrefix);
        _mySqlUnderlyingRegister = new(connectionString, _tablePrefix);
        Utilities = new Utilities(_mySqlUnderlyingRegister);
    }

    public async Task Initialize()
    {
        await _mySqlUnderlyingRegister.Initialize();
        await MessageStore.Initialize();
        await EffectsStore.Initialize();
        await StatesStore.Initialize();
        await TimeoutStore.Initialize();
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions (
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

        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropIfExists()
    {
        await _messageStore.DropUnderlyingTable();
        await _mySqlUnderlyingRegister.DropUnderlyingTable();
        await _timeoutStore.DropUnderlyingTable();

        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task TruncateTables()
    {
        await _messageStore.TruncateTable();
        await _timeoutStore.Truncate();
        await _mySqlUnderlyingRegister.TruncateTable();
        await _effectsStore.Truncate();
        await _statesStore.Truncate();
        
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $"TRUNCATE TABLE {_tablePrefix}rfunctions";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> CreateFunction(
        FunctionId functionId, 
        string? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp)
    {
        await using var conn = await CreateOpenConnection(_connectionString);

        var status = postponeUntil == null ? Status.Executing : Status.Postponed;
        var sql = @$"
            INSERT IGNORE INTO {_tablePrefix}rfunctions
                (function_type_id, function_instance_id, param_json, status, epoch, lease_expiration, postponed_until, timestamp)
            VALUES
                (?, ?, ?, ?, 0, ?, ?, ?)";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
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

    public async Task<StoredFunction?> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = @$"
            UPDATE {_tablePrefix}rfunctions
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
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND function_instance_id = ?;";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = leaseExpiration },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
            }
        };

        var reader = await command.ExecuteReaderAsync();
        if (reader.RecordsAffected == 0)
            return default;

        var sf = await ReadToStoredFunction(functionId, reader);
        return sf?.Epoch == expectedEpoch + 1
            ? sf
            : default;
    }

    public async Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET lease_expiration = ?
            WHERE function_type_id = ? AND function_instance_id = ? AND epoch = ? AND status = {(int) Status.Executing}";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = leaseExpiration},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = @$"
            SELECT function_instance_id, epoch, lease_expiration 
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND lease_expiration < ? AND status = {(int) Status.Executing}";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionTypeId.Value},
                new() { Value = leaseExpiresBefore }
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<StoredExecutingFunction>();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            var epoch = reader.GetInt32(1);
            var expiration = reader.GetInt64(2);
            functions.Add(new StoredExecutingFunction(functionInstanceId, epoch, expiration));
        }
        
        return functions;
    }

    public async Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long isEligibleBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = @$"
            SELECT function_instance_id, epoch, postponed_until
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND status = {(int) Status.Postponed} AND postponed_until <= ?";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionTypeId.Value},
                new() {Value = isEligibleBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var functions = new List<StoredPostponedFunction>();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            var epoch = reader.GetInt32(1);
            var postponedUntil = reader.GetInt64(2);
            functions.Add(new StoredPostponedFunction(functionInstanceId, epoch, postponedUntil));
        }
        
        return functions;
    }

    public async Task<bool> SetFunctionState(
        FunctionId functionId, Status status, 
        string? storedParameter, string? storedResult, 
        StoredException? storedException, 
        long? postponeUntil,
        int expectedEpoch)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
      
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = ?, 
                param_json = ?,  
                result_json = ?,  
                exception_json = ?, postponed_until = ?,
                epoch = epoch + 1
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = (int) status},
                new() {Value = storedParameter ?? (object) DBNull.Value},
                new() {Value = storedResult ?? (object) DBNull.Value},
                new() {Value = storedException != null ? JsonSerializer.Serialize(storedException) : DBNull.Value},
                new() {Value = postponeUntil ?? (object) DBNull.Value},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> SucceedFunction(
        FunctionId functionId, 
        string? result, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Succeeded}, result_json = ?, default_state = ?, timestamp = ?, epoch = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = result ?? (object)DBNull.Value },
                new() { Value = defaultState ?? (object)DBNull.Value },
                new() { Value = timestamp },
                new() { Value = expectedEpoch },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> PostponeFunction(
        FunctionId functionId, 
        long postponeUntil, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Postponed}, postponed_until = ?, default_state = ?, timestamp = ?, epoch = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = postponeUntil },
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = expectedEpoch },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> FailFunction(
        FunctionId functionId, 
        StoredException storedException, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Failed}, exception_json = ?, default_state = ?, timestamp = ?, epoch = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = JsonSerializer.Serialize(storedException) },
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = expectedEpoch },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> SuspendFunction(
        FunctionId functionId, 
        long expectedInterruptCount, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Suspended}, default_state = ?, timestamp = ?
            WHERE function_type_id = ? AND 
                  function_instance_id = ? AND 
                  epoch = ? AND
                  interrupt_count = ?;";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
                new() { Value = expectedInterruptCount }
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task SetDefaultState(FunctionId functionId, string? stateJson)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET default_state = ?
            WHERE function_type_id = ? AND function_instance_id = ?";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = stateJson ?? (object) DBNull.Value},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };

        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> SetParameters(
        FunctionId functionId,
        string? storedParameter, string? storedResult,
        int expectedEpoch)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
      
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET param_json = ?,  
                result_json = ?,
                epoch = epoch + 1
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = storedParameter ?? (object) DBNull.Value },
                new() { Value = storedResult ?? (object) DBNull.Value },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
            
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> IncrementInterruptCount(FunctionId functionId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET interrupt_count = interrupt_count + 1
            WHERE function_type_id = ? AND function_instance_id = ? AND status = {(int) Status.Executing};";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<long?> GetInterruptCount(FunctionId functionId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        var sql = $@"
            SELECT interrupt_count 
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND function_instance_id = ?;";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
            }
        };
        
        return (long?) await command.ExecuteScalarAsync();
    }

    public async Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            SELECT status, epoch
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND function_instance_id = ?;";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters = { 
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
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

    public async Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
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
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND function_instance_id = ?;";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters = { 
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        return await ReadToStoredFunction(functionId, reader);
    }

    private async Task<StoredFunction?> ReadToStoredFunction(FunctionId functionId, MySqlDataReader reader)
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
                functionId,
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

    public async Task DeleteFunction(FunctionId functionId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        var sql = $@"            
            DELETE FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND function_instance_id = ?";
        
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };

        await command.ExecuteNonQueryAsync();
        
        await _messageStore.Truncate(functionId);
        await _effectsStore.Remove(functionId);
        await _statesStore.Remove(functionId);
        await _timeoutStore.Remove(functionId);
    }
}
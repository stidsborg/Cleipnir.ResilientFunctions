using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MySQL;

public class MySqlTimeoutStore : ITimeoutStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public MySqlTimeoutStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix.ToLower();
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_timeouts (
                type VARCHAR(255),
                instance VARCHAR(255),
                timeout_id VARCHAR(255),
                expires BIGINT,
                PRIMARY KEY (type, instance, timeout_id),
                INDEX (expires, type, instance, timeout_id)
            )";
        var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_timeouts";
        var command = new MySqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _upsertTimeoutSql;
    private string? _insertTimeoutSql;
    public async Task UpsertTimeout(StoredTimeout storedTimeout, bool overwrite)
    {
        var (functionId, timeoutId, expiry) = storedTimeout;
        await using var conn = await CreateConnection();
        _upsertTimeoutSql ??= @$"
            INSERT INTO {_tablePrefix}_timeouts 
                (type, instance, timeout_id, expires)
            VALUES
                (?, ?, ?, ?) 
           ON DUPLICATE KEY UPDATE
                expires = ?";
        _insertTimeoutSql ??= @$"
                INSERT IGNORE INTO {_tablePrefix}_timeouts 
                    (type, instance, timeout_id, expires)
                VALUES
                    (?, ?, ?, ?)";

        var sql = overwrite ? _upsertTimeoutSql : _insertTimeoutSql;
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.Type.Value},
                new() {Value = functionId.Instance.Value},
                new() {Value = timeoutId},
                new() {Value = expiry},
                new() {Value = expiry}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _removeTimeoutSql;
    public async Task RemoveTimeout(FlowId flowId, string timeoutId)
    {
        await using var conn = await CreateConnection();
        _removeTimeoutSql ??= @$"
            DELETE FROM {_tablePrefix}_timeouts 
            WHERE 
                type = ? AND 
                instance = ? AND 
                timeout_id = ?";
        
        await using var command = new MySqlCommand(_removeTimeoutSql, conn)
        {
            Parameters =
            {
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value},
                new() {Value = timeoutId},
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _removeSql;
    public async Task Remove(FlowId flowId)
    {
        await using var conn = await CreateConnection();
        _removeSql ??= @$"
            DELETE FROM {_tablePrefix}_timeouts 
            WHERE type = ? AND instance = ?";
        
        await using var command = new MySqlCommand(_removeSql, conn)
        {
            Parameters =
            {
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value},
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _getTimeoutsSqlExpiresBefore;
    public async Task<IEnumerable<StoredTimeout>> GetTimeouts(long expiresBefore)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        _getTimeoutsSqlExpiresBefore ??= @$"    
            SELECT type, instance, timeout_id, expires
            FROM {_tablePrefix}_timeouts
            WHERE expires <= ?";
        
        await using var command = new MySqlCommand(_getTimeoutsSqlExpiresBefore, conn)
        {
            Parameters =
            {
                new() {Value = expiresBefore},
            }
        };
        
        var storedTimeouts = new List<StoredTimeout>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var flowType = reader.GetString(0);
            var flowInstance = reader.GetString(1);
            var timeoutId = reader.GetString(2);
            var expires = reader.GetInt64(3);
            var functionId = new FlowId(flowType, flowInstance);
            storedTimeouts.Add(new StoredTimeout(functionId, timeoutId, expires));
        }

        return storedTimeouts;
    }
    
    private string? _getFunctionTimeoutsSql;
    public async Task<IEnumerable<StoredTimeout>> GetTimeouts(FlowId flowId)
    {
        var (typeId, instanceId) = flowId;
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        _getFunctionTimeoutsSql ??= @$"    
            SELECT timeout_id, expires
            FROM {_tablePrefix}_timeouts
            WHERE type = ? AND instance = ?";
        
        await using var command = new MySqlCommand(_getFunctionTimeoutsSql, conn)
        {
            Parameters =
            {
                new() {Value = typeId.Value},
                new() {Value = instanceId.Value},
            }
        };
        
        var storedTimeouts = new List<StoredTimeout>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var timeoutId = reader.GetString(0);
            var expires = reader.GetInt64(1);
            storedTimeouts.Add(new StoredTimeout(flowId, timeoutId, expires));
        }

        return storedTimeouts;
    }

    private Task<MySqlConnection> CreateConnection() => DatabaseHelper.CreateOpenConnection(_connectionString);
}
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbTimeoutStore : ITimeoutStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public MariaDbTimeoutStore(string connectionString, string tablePrefix = "")
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
                type INT,
                instance CHAR(32),
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
                new() {Value = functionId.Instance.Value.ToString("N")},
                new() {Value = timeoutId},
                new() {Value = expiry},
                new() {Value = expiry}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _removeTimeoutSql;
    public async Task RemoveTimeout(StoredId storedId, string timeoutId)
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
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value.ToString("N")},
                new() {Value = timeoutId},
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _removeSql;
    public async Task Remove(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _removeSql ??= @$"
            DELETE FROM {_tablePrefix}_timeouts 
            WHERE type = ? AND instance = ?";
        
        await using var command = new MySqlCommand(_removeSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value.ToString("N")},
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
            var type = reader.GetInt32(0);
            var instance = reader.GetString(1).ToGuid().ToStoredInstance();
            var timeoutId = reader.GetString(2);
            var expires = reader.GetInt64(3);
            var functionId = new StoredId(new StoredType(type), instance);
            storedTimeouts.Add(new StoredTimeout(functionId, timeoutId, expires));
        }

        return storedTimeouts;
    }
    
    private string? _getFunctionTimeoutsSql;
    public async Task<IEnumerable<StoredTimeout>> GetTimeouts(StoredId storedId)
    {
        var (typeId, instanceId) = storedId;
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
                new() {Value = instanceId.Value.ToString("N")},
            }
        };
        
        var storedTimeouts = new List<StoredTimeout>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var timeoutId = reader.GetString(0);
            var expires = reader.GetInt64(1);
            storedTimeouts.Add(new StoredTimeout(storedId, timeoutId, expires));
        }

        return storedTimeouts;
    }

    private Task<MySqlConnection> CreateConnection() => DatabaseHelper.CreateOpenConnection(_connectionString);
}
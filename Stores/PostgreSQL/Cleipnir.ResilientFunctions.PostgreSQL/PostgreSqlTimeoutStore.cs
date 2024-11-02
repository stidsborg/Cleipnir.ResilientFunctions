using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlTimeoutStore(string connectionString, string tablePrefix = "") : ITimeoutStore
{
    private readonly string _tablePrefix = tablePrefix.ToLower();

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_timeouts (
                type INT,
                instance VARCHAR(255),
                timeout_id VARCHAR(255),
                expires BIGINT,
                PRIMARY KEY (type, instance, timeout_id)
            );

            CREATE INDEX IF NOT EXISTS idx_{_tablePrefix}_timeouts
            ON {_tablePrefix}_timeouts (expires, type, instance, timeout_id);
            ";
        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_timeouts";
        var command = new NpgsqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
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
                ($1, $2, $3, $4) 
            ON CONFLICT (type, instance, timeout_id) 
            DO UPDATE SET expires = EXCLUDED.expires";

        _insertTimeoutSql ??= @$"
            INSERT INTO {_tablePrefix}_timeouts 
                (type, instance, timeout_id, expires)
            VALUES
                ($1, $2, $3, $4) 
            ON CONFLICT DO NOTHING";

        var sql = overwrite ? _upsertTimeoutSql : _insertTimeoutSql;
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.Type.Value},
                new() {Value = functionId.Instance},
                new() {Value = timeoutId},
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
                type = $1 AND 
                instance = $2 AND
                timeout_id = $3";
        
        await using var command = new NpgsqlCommand(_removeTimeoutSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance},
                new() {Value = timeoutId}
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
            WHERE type = $1 AND instance = $2";
        
        await using var command = new NpgsqlCommand(_removeSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance},
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _getTimeoutsSqlExpiresBefore;
    public async Task<IEnumerable<StoredTimeout>> GetTimeouts(long expiresBefore)
    {
        await using var conn = await CreateConnection();
        _getTimeoutsSqlExpiresBefore ??= @$"
            SELECT type, instance, timeout_id, expires
            FROM {_tablePrefix}_timeouts 
            WHERE expires <= $1";
        
        await using var command = new NpgsqlCommand(_getTimeoutsSqlExpiresBefore, conn)
        {
            Parameters =
            {
                new() {Value = expiresBefore}
            }
        };

        var storedMessages = new List<StoredTimeout>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var flowType = reader.GetInt32(0);
            var flowInstance = reader.GetString(1);
            var timeoutId = reader.GetString(2);
            var expires = reader.GetInt64(3);
            var functionId = new StoredId(new StoredType(flowType), flowInstance);
            storedMessages.Add(new StoredTimeout(functionId, timeoutId, expires));
        }

        return storedMessages;
    }

    private string? _getTimeoutsSql;    
    public async Task<IEnumerable<StoredTimeout>> GetTimeouts(StoredId flowId)
    {
        var (typeId, instanceId) = flowId;
        await using var conn = await CreateConnection();
        _getTimeoutsSql ??= @$"
            SELECT timeout_id, expires
            FROM {_tablePrefix}_timeouts 
            WHERE type = $1 AND instance = $2";
        
        await using var command = new NpgsqlCommand(_getTimeoutsSql, conn)
        {
            Parameters =
            {
                new() {Value = typeId.Value},
                new() {Value = instanceId}
            }
        };

        var storedMessages = new List<StoredTimeout>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var timeoutId = reader.GetString(0);
            var expires = reader.GetInt64(1);
            storedMessages.Add(new StoredTimeout(flowId, timeoutId, expires));
        }

        return storedMessages;
    }
}
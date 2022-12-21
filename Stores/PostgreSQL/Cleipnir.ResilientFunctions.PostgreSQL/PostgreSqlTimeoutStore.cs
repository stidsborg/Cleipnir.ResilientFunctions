using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlTimeoutStore : ITimeoutStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public PostgreSqlTimeoutStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix.ToLower();
    } 
    
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions_timeouts (
                function_type_id VARCHAR(255),
                function_instance_id VARCHAR(255),
                timeout_id VARCHAR(255),
                expires BIGINT,
                PRIMARY KEY (function_type_id, function_instance_id, timeout_id)
            )";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        var sql = @$"TRUNCATE TABLE {_tablePrefix}rfunctions_timeouts";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
    
    public async Task UpsertTimeout(StoredTimeout storedTimeout)
    {
        var (functionId, timeoutId, expiry) = storedTimeout;
        await using var conn = await CreateConnection();
        var sql = @$"
            INSERT INTO {_tablePrefix}rfunctions_timeouts 
                (function_type_id, function_instance_id, timeout_id, expires)
            VALUES
                ($1, $2, $3, $4) 
            ON CONFLICT (function_type_id, function_instance_id, timeout_id) 
            DO UPDATE SET expires = EXCLUDED.expires";
        
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = timeoutId},
                new() {Value = expiry}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task RemoveTimeout(FunctionId functionId, string timeoutId)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            DELETE FROM {_tablePrefix}rfunctions_timeouts 
            WHERE 
                function_type_id = $1 AND 
                function_instance_id = $2 AND
                timeout_id = $3";
        
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = timeoutId}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task<IEnumerable<StoredTimeout>> GetTimeouts(string functionTypeId, long expiresBefore)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT function_instance_id, timeout_id, expires
            FROM {_tablePrefix}rfunctions_timeouts 
            WHERE 
                function_type_id = $1 AND 
                expires <= $2";
        
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionTypeId},
                new() {Value = expiresBefore}
            }
        };

        var storedEvents = new List<StoredTimeout>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            var timeoutId = reader.GetString(1);
            var expires = reader.GetInt64(2);
            var functionId = new FunctionId(functionTypeId, functionInstanceId);
            storedEvents.Add(new StoredTimeout(functionId, timeoutId, expires));
        }

        return storedEvents;
    }
}
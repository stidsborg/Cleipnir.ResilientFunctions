﻿using Cleipnir.ResilientFunctions.Domain;
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
        var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        var sql = @$"TRUNCATE TABLE {_tablePrefix}rfunctions_timeouts";
        var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task UpsertTimeout(StoredTimeout storedTimeout, bool overwrite)
    {
        var (functionId, timeoutId, expiry) = storedTimeout;
        await using var conn = await CreateConnection();
        var sql = @$"
            INSERT INTO {_tablePrefix}rfunctions_timeouts 
                (function_type_id, function_instance_id, timeout_id, expires)
            VALUES
                (?, ?, ?, ?) 
           ON DUPLICATE KEY UPDATE
                expires = ?";
        
        if (!overwrite)
            sql = @$"
                INSERT IGNORE INTO {_tablePrefix}rfunctions_timeouts 
                    (function_type_id, function_instance_id, timeout_id, expires)
                VALUES
                    (?, ?, ?, ?)";
        
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = timeoutId},
                new() {Value = expiry},
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
                function_type_id = ? AND 
                function_instance_id = ? AND 
                timeout_id = ?";
        
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = timeoutId},
            }
        };

        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<IEnumerable<StoredTimeout>> GetTimeouts(string functionTypeId, long expiresBefore)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var sql = @$"    
            SELECT function_instance_id, timeout_id, expires
            FROM {_tablePrefix}rfunctions_timeouts
            WHERE function_type_id = ? AND expires <= ?";
        
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionTypeId},
                new() {Value = expiresBefore},
            }
        };
        
        var storedTimeouts = new List<StoredTimeout>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            var timeoutId = reader.GetString(1);
            var expires = reader.GetInt64(2);
            var functionId = new FunctionId(functionTypeId, functionInstanceId);
            storedTimeouts.Add(new StoredTimeout(functionId, timeoutId, expires));
        }

        return storedTimeouts;
    }

    public async Task<IEnumerable<StoredTimeout>> GetTimeouts(FunctionId functionId)
    {
        var (typeId, instanceId) = functionId;
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var sql = @$"    
            SELECT timeout_id, expires
            FROM {_tablePrefix}rfunctions_timeouts
            WHERE function_type_id = ? AND function_instance_id = ?";
        
        await using var command = new MySqlCommand(sql, conn)
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
            storedTimeouts.Add(new StoredTimeout(functionId, timeoutId, expires));
        }

        return storedTimeouts;
    }

    private Task<MySqlConnection> CreateConnection() => DatabaseHelper.CreateOpenConnection(_connectionString);

    public async Task DropUnderlyingTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions_timeouts";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
}
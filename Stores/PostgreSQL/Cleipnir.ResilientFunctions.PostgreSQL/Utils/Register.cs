using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils.Register;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Utils;

public class Register : IRegister
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public Register(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }
    
    public async Task Initialize()
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        var sql = @$"            
                CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions_register (
                    groupName VARCHAR(255) NOT NULL,
                    keyId VARCHAR(255) NOT NULL,
                    value VARCHAR(255) NOT NULL,
                    PRIMARY KEY (groupName, keyId)
                );";
        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropUnderlyingTable()
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions_register";
        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> SetIfEmpty(string group, string key, string value)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = @$" 
            INSERT INTO {_tablePrefix}rfunctions_register
                (groupName, keyId, value)
            VALUES
                ($1, $2, $3)
            ON CONFLICT DO NOTHING";
        
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = group},
                new() {Value = key}, 
                new() {Value = value}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> CompareAndSwap(string group, string key, string newValue, string expectedValue, bool setIfEmpty = true)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        if (!setIfEmpty)
        {
            //as setIfEmpty is false then only update if expected value is found
            var sql = @$" 
                UPDATE {_tablePrefix}rfunctions_register
                SET value = $1
                WHERE groupName = $2 AND keyId = $3 AND value = $4";
        
            await using var command = new NpgsqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() {Value = newValue},
                    new() {Value = group},
                    new() {Value = key}, 
                    new() {Value = expectedValue},
                }
            };

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows > 0;
        }
        else
        {
            //setIfEmpty is true
            await using var batch = new NpgsqlBatch(conn);
            {
                var command = 
                    new NpgsqlBatchCommand($"DELETE FROM {_tablePrefix}rfunctions_register WHERE groupName = $1 AND keyId = $2 AND value = $3")
                    {
                        Parameters =
                        {
                            new() { Value = group },
                            new() { Value = key },
                            new() { Value = expectedValue },
                        }
                    };
                batch.BatchCommands.Add(command);
            }
            {
                var sql = @$" 
                    INSERT INTO {_tablePrefix}rfunctions_register
                        (groupName, keyId, value)
                    VALUES
                        ($1, $2, $3)
                    ON CONFLICT DO NOTHING";

                var command = new NpgsqlBatchCommand(sql)
                {
                    Parameters =
                    {
                        new() { Value = group },
                        new() { Value = key },
                        new() { Value = newValue }
                    }
                };
            
                batch.BatchCommands.Add(command);
            }
            
            var affectedRows = await batch.ExecuteNonQueryAsync(); 
            return affectedRows > 0;   
        }
    }

    public async Task<string?> Get(string group, string key)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        
        var sql = @$"    
            SELECT value
            FROM {_tablePrefix}rfunctions_register
            WHERE groupName = $1 AND keyId = $2";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = group},
                new() {Value = key}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            return reader.GetString(0);

        return default;
    }

    public async Task<bool> Delete(string group, string key, string expectedValue)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = @$" 
            DELETE FROM {_tablePrefix}rfunctions_register
            WHERE groupName = $1 AND keyId = $2 AND value = $3";

        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = group },
                new() { Value = key },
                new() { Value = expectedValue },
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task Delete(string group, string key)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = @$" 
            DELETE FROM {_tablePrefix}rfunctions_register
            WHERE groupName = $1 AND keyId = $2";

        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = group },
                new() { Value = key },
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task<bool> Exists(string group, string key)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        
        var sql = @$"    
            SELECT COUNT(*)
            FROM {_tablePrefix}rfunctions_register
            WHERE groupName = $1 AND keyId = $2";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = group},
                new() {Value = key}
            }
        };
        
        var count = (long?) await command.ExecuteScalarAsync();
        return count > 0;
    }
}
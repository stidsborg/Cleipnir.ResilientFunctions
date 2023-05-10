using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgresSqlUnderlyingRegister : IUnderlyingRegister
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public PostgresSqlUnderlyingRegister(string connectionString, string tablePrefix = "")
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
                    registertype INT NOT NULL,
                    groupname VARCHAR(255) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    value VARCHAR(255) NOT NULL,
                    PRIMARY KEY (registertype, groupname, name)
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
    
    public async Task<bool> SetIfEmpty(RegisterType registerType, string group, string name, string value)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = @$" 
            INSERT INTO {_tablePrefix}rfunctions_register
                (registertype, groupname, name, value)
            VALUES
                ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING";
        
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = (int) registerType },
                new() {Value = group},
                new() {Value = name}, 
                new() {Value = value}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> CompareAndSwap(RegisterType registerType, string group, string name, string newValue, string expectedValue, bool setIfEmpty = true)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        if (!setIfEmpty)
        {
            //as setIfEmpty is false then only update if expected value is found
            var sql = @$" 
                UPDATE {_tablePrefix}rfunctions_register
                SET value = $1
                WHERE registertype = $2 AND groupname = $3 AND name = $4 AND value = $5";
        
            await using var command = new NpgsqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() {Value = newValue},
                    new() {Value = (int) registerType},
                    new() {Value = group},
                    new() {Value = name}, 
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
                    new NpgsqlBatchCommand($"DELETE FROM {_tablePrefix}rfunctions_register WHERE registertype = $1 AND groupname = $2 AND name = $3 AND value = $4")
                    {
                        Parameters =
                        {
                            new() { Value = (int) registerType },
                            new() { Value = group },
                            new() { Value = name },
                            new() { Value = expectedValue },
                        }
                    };
                batch.BatchCommands.Add(command);
            }
            {
                var sql = @$" 
                    INSERT INTO {_tablePrefix}rfunctions_register
                        (registertype, groupname, name, value)
                    VALUES
                        ($1, $2, $3, $4)
                    ON CONFLICT DO NOTHING";

                var command = new NpgsqlBatchCommand(sql)
                {
                    Parameters =
                    {
                        new() { Value = (int) registerType },
                        new() { Value = group },
                        new() { Value = name },
                        new() { Value = newValue }
                    }
                };
            
                batch.BatchCommands.Add(command);
            }
            
            var affectedRows = await batch.ExecuteNonQueryAsync(); 
            return affectedRows > 0;   
        }
    }

    public async Task<string?> Get(RegisterType registerType, string group, string key)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        
        var sql = @$"    
            SELECT value
            FROM {_tablePrefix}rfunctions_register
            WHERE registertype = $1 AND groupname = $2 AND name = $3";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = (int) registerType},
                new() {Value = group},
                new() {Value = key}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            return reader.GetString(0);

        return default;
    }

    public async Task<bool> Delete(RegisterType registerType, string group, string name, string expectedValue)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = @$" 
            DELETE FROM {_tablePrefix}rfunctions_register
            WHERE registertype = $1 AND groupname = $2 AND name = $3 AND value = $4";

        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = (int) registerType},
                new() { Value = group },
                new() { Value = name },
                new() { Value = expectedValue },
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task Delete(RegisterType registerType, string group, string name)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = @$" 
            DELETE FROM {_tablePrefix}rfunctions_register
            WHERE registertype = $1 AND groupname = $2 AND name = $3";

        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = (int) registerType },
                new() { Value = group },
                new() { Value = name },
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task<bool> Exists(RegisterType registerType, string group, string name)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        
        var sql = @$"    
            SELECT COUNT(*)
            FROM {_tablePrefix}rfunctions_register
            WHERE registertype = $1 AND groupname = $2 AND name = $3";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = (int) registerType},
                new() {Value = group},
                new() {Value = name}
            }
        };
        
        var count = (long?) await command.ExecuteScalarAsync();
        return count > 0;
    }
}
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

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        _initializeSql ??= @$"            
                CREATE TABLE IF NOT EXISTS {_tablePrefix}_register (
                    registertype INT NOT NULL,
                    groupname VARCHAR(255) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    value VARCHAR(255) NOT NULL,
                    PRIMARY KEY (registertype, groupname, name)
                );";
        await using var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _dropUnderlyingTableSql;
    public async Task DropUnderlyingTable()
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        _dropUnderlyingTableSql ??= $"DROP TABLE IF EXISTS {_tablePrefix}_register";
        await using var command = new NpgsqlCommand(_dropUnderlyingTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _setIfEmptySql;
    public async Task<bool> SetIfEmpty(RegisterType registerType, string group, string name, string value)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        _setIfEmptySql ??= @$" 
            INSERT INTO {_tablePrefix}_register
                (registertype, groupname, name, value)
            VALUES
                ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING";
        
        await using var command = new NpgsqlCommand(_setIfEmptySql, conn)
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

    private string? _compareAndSwapUpdateSql;
    private string? _compareAndSwapDeleteExistingSql;
    private string? _compareAndSwapInsertSql;
    public async Task<bool> CompareAndSwap(RegisterType registerType, string group, string name, string newValue, string expectedValue, bool setIfEmpty = true)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        if (!setIfEmpty)
        {
            //as setIfEmpty is false then only update if expected value is found
            _compareAndSwapUpdateSql ??= @$" 
                UPDATE {_tablePrefix}_register
                SET value = $1
                WHERE registertype = $2 AND groupname = $3 AND name = $4 AND value = $5";
        
            await using var command = new NpgsqlCommand(_compareAndSwapUpdateSql, conn)
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
                _compareAndSwapDeleteExistingSql ??= @$"
                    DELETE FROM {_tablePrefix}_register 
                    WHERE registertype = $1 AND groupname = $2 AND name = $3 AND value = $4";
                var command =
                    new NpgsqlBatchCommand(_compareAndSwapDeleteExistingSql)
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
                _compareAndSwapInsertSql ??= @$" 
                    INSERT INTO {_tablePrefix}_register
                        (registertype, groupname, name, value)
                    VALUES
                        ($1, $2, $3, $4)
                    ON CONFLICT DO NOTHING";

                var command = new NpgsqlBatchCommand(_compareAndSwapInsertSql)
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

    private string? _getSql;
    public async Task<string?> Get(RegisterType registerType, string group, string key)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        
        _getSql ??= @$"    
            SELECT value
            FROM {_tablePrefix}_register
            WHERE registertype = $1 AND groupname = $2 AND name = $3";
        await using var command = new NpgsqlCommand(_getSql, conn)
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

    private string? _deleteExpectedValueSql;
    public async Task<bool> Delete(RegisterType registerType, string group, string name, string expectedValue)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        _deleteExpectedValueSql ??= @$" 
            DELETE FROM {_tablePrefix}_register
            WHERE registertype = $1 AND groupname = $2 AND name = $3 AND value = $4";

        await using var command = new NpgsqlCommand(_deleteExpectedValueSql, conn)
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

    private string? _deleteSql;
    public async Task Delete(RegisterType registerType, string group, string name)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        _deleteSql ??= @$" 
            DELETE FROM {_tablePrefix}_register
            WHERE registertype = $1 AND groupname = $2 AND name = $3";

        await using var command = new NpgsqlCommand(_deleteSql, conn)
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

    private string? _existsSql;
    public async Task<bool> Exists(RegisterType registerType, string group, string name)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        
        _existsSql ??= @$"    
            SELECT COUNT(*)
            FROM {_tablePrefix}_register
            WHERE registertype = $1 AND groupname = $2 AND name = $3";
        await using var command = new NpgsqlCommand(_existsSql, conn)
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

    private string? _truncateTableSql;
    public async Task TruncateTable()
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        
        _truncateTableSql ??= $"TRUNCATE TABLE {_tablePrefix}_register";
        var command = new NpgsqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }
}
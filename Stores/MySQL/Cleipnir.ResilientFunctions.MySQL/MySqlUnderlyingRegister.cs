using Cleipnir.ResilientFunctions.Utils;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MySQL;

public class MySqlUnderlyingRegister : IUnderlyingRegister
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public MySqlUnderlyingRegister(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions_register (
                registertype INT NOT NULL,
                `group` VARCHAR(255) NOT NULL,                
                name VARCHAR(255) NOT NULL,
                value VARCHAR(1024) NOT NULL,
                PRIMARY KEY (registertype, `group`, name)
            );";

        await using var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _setIfEmptySql;
    public async Task<bool> SetIfEmpty(RegisterType registerType, string group, string name, string value)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _setIfEmptySql ??= @$"
            INSERT IGNORE INTO {_tablePrefix}rfunctions_register
                (registertype, `group`, name, value)   
            VALUES
                (?, ?, ?, ?);";
        
        await using var command = new MySqlCommand(_setIfEmptySql, conn)
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
    private string? _compareAndSwapUpsertSql;
    public async Task<bool> CompareAndSwap(RegisterType registerType, string group, string name, string newValue, string expectedValue, bool setIfEmpty = true)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        
        if (!setIfEmpty)
        {
            _compareAndSwapUpdateSql ??= @$"
            UPDATE {_tablePrefix}rfunctions_register
            SET value = ?
            WHERE registertype = ? AND `group` = ? AND name = ? AND value = ?;";
            
            await using var command = new MySqlCommand(_compareAndSwapUpdateSql, conn)
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
            _compareAndSwapUpsertSql ??= @$"
            START TRANSACTION;
            DELETE FROM {_tablePrefix}rfunctions_register WHERE registertype = ? AND `group` = ? AND name = ? AND value = ?;
            INSERT IGNORE INTO {_tablePrefix}rfunctions_register
                (registertype, `group`, name, value)   
            VALUES
                (?, ?, ?, ?);
            COMMIT;";

            await using var command = new MySqlCommand(_compareAndSwapUpsertSql, conn)
            {
                Parameters =
                {
                    new() { Value = (int) registerType},
                    new() { Value = group },
                    new() { Value = name },
                    new() { Value = expectedValue },
                    new() { Value = (int) registerType},
                    new() { Value = group },
                    new() { Value = name },
                    new() { Value = newValue }
                }
            };

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows > 0;
        }
    }

    private string? _getSql;
    public async Task<string?> Get(RegisterType registerType, string group, string name)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        _getSql ??= @$"    
            SELECT value 
            FROM {_tablePrefix}rfunctions_register  
            WHERE registertype = ? AND `group` = ? AND name = ?;";
       
        await using var command = new MySqlCommand(_getSql, conn)
        {
            Parameters =
            {
                new() { Value = (int) registerType },
                new() { Value = group },
                new() { Value = name }
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            return reader.GetString(0);
 
        return default;
    }

    private string? _conditionalDeleteSql;
    public async Task<bool> Delete(RegisterType registerType, string group, string name, string expectedValue)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);

        _conditionalDeleteSql ??= $"DELETE FROM {_tablePrefix}rfunctions_register WHERE registertype = ? AND `group` = ? AND name = ? AND value = ?;";

        await using var command = new MySqlCommand(_conditionalDeleteSql, conn)
        {
            Parameters =
            {
                new() {Value = (int) registerType},
                new() {Value = group},
                new() {Value = name},
                new() {Value = expectedValue},
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    private string? _deleteSql;
    public async Task Delete(RegisterType registerType, string group, string name)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);

        _deleteSql ??= $"DELETE FROM {_tablePrefix}rfunctions_register WHERE registertype = ? AND `group` = ? AND name = ?;";

        await using var command = new MySqlCommand(_deleteSql, conn)
        {
            Parameters =
            {
                new() {Value = (int) registerType},
                new() {Value = group},
                new() {Value = name}
            }
        };
        await command.ExecuteScalarAsync();
    }

    private string? _existsSql;
    public async Task<bool> Exists(RegisterType registerType, string group, string name)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        _existsSql ??= @$"    
            SELECT COUNT(*)
            FROM {_tablePrefix}rfunctions_register  
            WHERE registertype = ? AND `group` = ? AND name = ?;";
       
        await using var command = new MySqlCommand(_existsSql, conn)
        {
            Parameters =
            {
                new() {Value = (int) registerType},
                new() {Value = group},
                new() {Value = name}
            }
        };
        
        var count = (long) (await command.ExecuteScalarAsync() ?? 0);
        return count > 0;
    }

    private string? _dropUnderlyingTableSql;
    public async Task DropUnderlyingTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _dropUnderlyingTableSql ??= $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions_register";
        
        await using var command = new MySqlCommand(_dropUnderlyingTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateTableSql;
    public async Task TruncateTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _truncateTableSql ??= $"TRUNCATE TABLE {_tablePrefix}rfunctions_register";
        var command = new MySqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }
}
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

    public async Task Initialize()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions_register (
                registertype INT NOT NULL,
                `group` VARCHAR(255) NOT NULL,                
                name VARCHAR(255) NOT NULL,
                value VARCHAR(1024) NOT NULL,
                PRIMARY KEY (registertype, `group`, name)
            );";

        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> SetIfEmpty(RegisterType registerType, string group, string name, string value)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = @$"
            INSERT IGNORE INTO {_tablePrefix}rfunctions_register
                (registertype, `group`, name, value)   
            VALUES
                (?, ?, ?, ?);";
        
        await using var command = new MySqlCommand(sql, conn)
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
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        
        if (!setIfEmpty)
        {
            var sql = @$"
            UPDATE {_tablePrefix}rfunctions_register
            SET value = ?
            WHERE registertype = ? AND `group` = ? AND name = ? AND value = ?;";
            
            await using var command = new MySqlCommand(sql, conn)
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
            var sql = @$"
            START TRANSACTION;
            DELETE FROM {_tablePrefix}rfunctions_register WHERE registertype = ? AND `group` = ? AND name = ? AND value = ?;
            INSERT IGNORE INTO {_tablePrefix}rfunctions_register
                (registertype, `group`, name, value)   
            VALUES
                (?, ?, ?, ?);
            COMMIT;";

            await using var command = new MySqlCommand(sql, conn)
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

    public async Task<string?> Get(RegisterType registerType, string group, string name)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var sql = @$"    
            SELECT value 
            FROM {_tablePrefix}rfunctions_register  
            WHERE registertype = ? AND `group` = ? AND name = ?;";
       
        await using var command = new MySqlCommand(sql, conn)
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

    public async Task<bool> Delete(RegisterType registerType, string group, string name, string expectedValue)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);

        var sql = @$"DELETE FROM {_tablePrefix}rfunctions_register WHERE registertype = ? AND `group` = ? AND name = ? AND value = ?;";

        await using var command = new MySqlCommand(sql, conn)
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

    public async Task Delete(RegisterType registerType, string group, string name)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);

        var sql = @$"DELETE FROM {_tablePrefix}rfunctions_register WHERE registertype = ? AND `group` = ? AND name = ?;";

        await using var command = new MySqlCommand(sql, conn)
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

    public async Task<bool> Exists(RegisterType registerType, string group, string name)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var sql = @$"    
            SELECT COUNT(*)
            FROM {_tablePrefix}rfunctions_register  
            WHERE registertype = ? AND `group` = ? AND name = ?;";
       
        await using var command = new MySqlCommand(sql, conn)
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

    public async Task DropUnderlyingTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions_register";
        
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task TruncateTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = $"TRUNCATE TABLE {_tablePrefix}rfunctions_register";
        var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
}
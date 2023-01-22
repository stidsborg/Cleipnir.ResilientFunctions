using Cleipnir.ResilientFunctions.Utils.Register;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MySQL.Utils;

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
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions_register (
                groupname VARCHAR(255) NOT NULL,                
                keyid VARCHAR(255) NOT NULL,
                value VARCHAR(1024) NOT NULL,
                PRIMARY KEY (groupname, keyid)
            );";

        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> SetIfEmpty(string group, string key, string value)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = @$"
            INSERT IGNORE INTO {_tablePrefix}rfunctions_register
                (groupname, keyid, value)   
            VALUES
                (?, ?, ?);";
        
        await using var command = new MySqlCommand(sql, conn)
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
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        
        if (!setIfEmpty)
        {
            var sql = @$"
            UPDATE {_tablePrefix}rfunctions_register
            SET value = ?
            WHERE groupname = ? AND keyid = ? AND value = ?;";
            
            await using var command = new MySqlCommand(sql, conn)
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
            var sql = @$"
            START TRANSACTION;
            DELETE FROM {_tablePrefix}rfunctions_register WHERE groupname = ? AND keyid = ? AND value = ?;
            INSERT IGNORE INTO {_tablePrefix}rfunctions_register
                (groupname, keyid, value)   
            VALUES
                (?, ?, ?);
            COMMIT;";

            await using var command = new MySqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() { Value = group },
                    new() { Value = key },
                    new() { Value = expectedValue },
                    new() { Value = group },
                    new() { Value = key },
                    new() { Value = newValue }
                }
            };

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows > 0;
        }
    }

    public async Task<string?> Get(string group, string key)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var sql = @$"    
            SELECT value 
            FROM {_tablePrefix}rfunctions_register  
            WHERE groupname = ? AND keyid = ?;";
       
        await using var command = new MySqlCommand(sql, conn)
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
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);

        var sql = @$"DELETE FROM {_tablePrefix}rfunctions_register WHERE groupname = ? AND keyid = ? AND value = ?;";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = group},
                new() {Value = key},
                new() {Value = expectedValue},
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task Delete(string group, string key)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);

        var sql = @$"DELETE FROM {_tablePrefix}rfunctions_register WHERE groupname = ? AND keyid = ?;";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = group},
                new() {Value = key}
            }
        };
        await command.ExecuteScalarAsync();
    }

    public async Task<bool> Exists(string group, string key)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var sql = @$"    
            SELECT COUNT(*)
            FROM {_tablePrefix}rfunctions_register  
            WHERE groupname = ? AND keyid = ?;";
       
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = group},
                new() {Value = key}
            }
        };
        
        var count = (long) (await command.ExecuteScalarAsync() ?? 0);
        return count > 0;
    }

    public async Task DropUnderlyingTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = @$"DROP TABLE IF EXISTS {_tablePrefix}rfunctions_register";
        
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
}
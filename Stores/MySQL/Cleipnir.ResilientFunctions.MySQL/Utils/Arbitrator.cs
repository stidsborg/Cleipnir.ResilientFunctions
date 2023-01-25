using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MySQL.Utils;

public class Arbitrator : IArbitrator
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public Arbitrator(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    public async Task Initialize()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = @$"            
                CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions_arbitrator (
                    groupName VARCHAR(255) NOT NULL,
                    keyId VARCHAR(255) NOT NULL,
                    value VARCHAR(255) NOT NULL,
                    PRIMARY KEY (groupName, keyId)
                );";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropUnderlyingTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);

        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions_arbitrator";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public Task<bool> Propose(string group, string key, string value)
        => InnerPropose(group, key, value);
    
    public Task<bool> Propose(string key, string value)
        => InnerPropose(group: "", key, value);
    
    private async Task<bool> InnerPropose(string group, string key, string value)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);

        var sql = $@"
            INSERT IGNORE INTO {_tablePrefix}rfunctions_arbitrator
                (groupName, keyId, value)
            VALUES
                (?, ?, ?)";
        {
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
            if (affectedRows == 1) return true;    
        }
        {
            sql = @$"SELECT COUNT(*) FROM {_tablePrefix}rfunctions_arbitrator WHERE groupName=? AND keyId=? AND value=?";
            await using var command = new MySqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() {Value = group},
                    new() {Value = key},
                    new() {Value = value}
                }
            };

            var countedRows = (long) (await command.ExecuteScalarAsync() ?? 0);
            return countedRows == 1;
        }
    }

    public Task Delete(string key) => InnerDelete(group: "", key);
    public Task Delete(string group, string key) => InnerDelete(group, key);

    private async Task InnerDelete(string group, string key)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);

        var sql = $@"DELETE FROM {_tablePrefix}rfunctions_arbitrator WHERE groupName=? AND keyId=?";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = group},
                new() {Value = key},
            }
        };

        await command.ExecuteNonQueryAsync();
    }
}
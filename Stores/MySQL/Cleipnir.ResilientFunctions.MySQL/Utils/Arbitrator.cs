using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using MySql.Data.MySqlClient;

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
                CREATE TABLE IF NOT EXISTS {_tablePrefix}arbitrator (
                    id VARCHAR(255) PRIMARY KEY NOT NULL,                
                    value VARCHAR(255) NOT NULL
                );";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public Task<bool> Propose(string groupId, string instanceId, string value)
        => InnerPropose(groupId, instanceId, value);
    
    public Task<bool> Propose(string groupId, string value)
        => InnerPropose(groupId, instanceId: null, value);
    
    private async Task<bool> InnerPropose(string groupId, string? instanceId, string value)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var id = KeyEncoder.Encode(groupId, instanceId);

        var sql = $@"
            INSERT IGNORE INTO {_tablePrefix}arbitrator
                (id, value)
            VALUES
                (?, ?)";
        {
            await using var command = new MySqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() {Value = id},
                    new() {Value = value}
                }
            };

            var affectedRows = await command.ExecuteNonQueryAsync();
            if (affectedRows == 1) return true;    
        }
        {
            sql = @$"SELECT COUNT(*) FROM {_tablePrefix}arbitrator WHERE id=? AND value=?";
            await using var command = new MySqlCommand(sql, conn)
            {
                Parameters =
                {
                    new() {Value = id},
                    new() {Value = value}
                }
            };

            var countedRows = (long) (await command.ExecuteScalarAsync() ?? 0);
            return countedRows == 1;
        }
    }

    public Task Delete(string groupId) => InnerDelete(groupId, instanceId: null);
    public Task Delete(string groupId, string instanceId) => InnerDelete(groupId, instanceId);

    private async Task InnerDelete(string groupId, string? instanceId)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var id = KeyEncoder.Encode(groupId, instanceId);

        var sql = $@"DELETE FROM {_tablePrefix}arbitrator WHERE id=?";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = id },
            }
        };

        await command.ExecuteNonQueryAsync();
    }
}
using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Utils;

public class Arbitrator : IArbitrator
{
    private readonly Func<Task<NpgsqlConnection>> _connFunc;
    private readonly string _tablePrefix;

    public Arbitrator(Func<Task<NpgsqlConnection>> connFunc, string tablePrefix = "")
    {
        _connFunc = connFunc;
        _tablePrefix = tablePrefix;
    }

    public async Task Initialize()
    {
        await using var conn = await _connFunc();
        var sql = @$"            
                CREATE TABLE IF NOT EXISTS {_tablePrefix}arbitrator (
                    id VARCHAR(255) PRIMARY KEY NOT NULL,                
                    value VARCHAR(255) NOT NULL
                );";
        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public Task<bool> Propose(string groupId, string instanceId, string value)
        => InnerPropose(groupId, instanceId, value);
    
    public Task<bool> Propose(string groupId, string value)
        => InnerPropose(groupId, instanceId: null, value);
    
    private async Task<bool> InnerPropose(string groupId, string? instanceId, string value)
    {
        await using var conn = await _connFunc();
        var id = KeyEncoder.Encode(groupId, instanceId);

        var sql = $@"
            INSERT INTO {_tablePrefix}arbitrator
                (id, value)
            VALUES
                ($1, $2)
            ON CONFLICT DO NOTHING";
        {
            await using var command = new NpgsqlCommand(sql, conn)
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
            sql = @$"SELECT COUNT(*) FROM {_tablePrefix}arbitrator WHERE id=$1 AND value=$2";
            await using var command = new NpgsqlCommand(sql, conn)
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
        await using var conn = await _connFunc();
        var id = KeyEncoder.Encode(groupId, instanceId);

        var sql = $@"DELETE FROM {_tablePrefix}arbitrator WHERE id = $1";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = id }
            }
        };
        await command.ExecuteNonQueryAsync();
    }
}
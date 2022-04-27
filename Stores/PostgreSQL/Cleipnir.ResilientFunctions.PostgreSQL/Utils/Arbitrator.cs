using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Dapper;
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
        await conn.ExecuteAsync(@$"            
                CREATE TABLE IF NOT EXISTS {_tablePrefix}arbitrator (
                    id VARCHAR(255) PRIMARY KEY NOT NULL,                
                    value VARCHAR(255) NOT NULL
                );"
        );
    }
    
    public async Task DropUnderlyingTable()
    {
        await using var conn = await _connFunc();
        await conn.ExecuteAsync(@$"DROP TABLE IF NOT EXISTS {_tablePrefix}arbitrator");
    }

    public Task<bool> Propose(string groupId, string instanceId, string value)
        => InnerPropose(groupId, instanceId, value);
    
    public Task<bool> Propose(string groupId, string value)
        => InnerPropose(groupId, instanceId: null, value);
    
    private async Task<bool> InnerPropose(string groupId, string? instanceId, string value)
    {
        await using var conn = await _connFunc();
        var id = KeyEncoder.Encode(groupId, instanceId);

        var affectedRows = await conn.ExecuteAsync($@"
            INSERT INTO {_tablePrefix}arbitrator
                (id, value)
            VALUES
                (@Id, @Value)
            ON CONFLICT DO NOTHING",
            new { Id = id, Value = value }
        );
        if (affectedRows == 1) return true;
        
        var existingValue = await conn.QuerySingleAsync<string>(
            @$"SELECT value FROM {_tablePrefix}arbitrator WHERE id=@Id",
            new {Id = id}
        );

        return existingValue == value;
    }
}
using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer.Utils;

public class Arbitrator : IArbitrator
{
    private readonly Func<Task<SqlConnection>> _connFunc;
    private readonly string _tablePrefix;

    public Arbitrator(Func<Task<SqlConnection>> connFunc, string tablePrefix = "")
    {
        _connFunc = connFunc;
        _tablePrefix = tablePrefix;
    }

    public async Task Initialize()
    {
        await using var conn = await _connFunc();
        try
        {
            await conn.ExecuteAsync(@$"            
                CREATE TABLE {_tablePrefix}Arbitrator (
                    [Id] NVARCHAR(255) PRIMARY KEY NOT NULL,                
                    [Value] NVARCHAR(255) NOT NULL
                );"
            );
        }
        catch (SqlException e)
        {
            if (e.Number != SqlError.TABLE_ALREADY_EXISTS)
                throw;
        }
    }
    
    public async Task DropUnderlyingTable()
    {
        await using var conn = await _connFunc();
        try
        {
            await conn.ExecuteAsync(@$"DROP TABLE {_tablePrefix}Arbitrator");
        }
        catch (SqlException e)
        {
            if (e.Number != SqlError.TABLE_DOES_NOT_EXIST)
                throw;
        }
    }

    public Task<bool> Propose(string groupId, string instanceId, string value)
        => InnerPropose(groupId, instanceId, value);
    
    public Task<bool> Propose(string groupId, string value)
        => InnerPropose(groupId, instanceId: null, value);
    
    private async Task<bool> InnerPropose(string groupId, string? instanceId, string value)
    {
        await using var conn = await _connFunc();
        var id = KeyEncoder.Encode(groupId, instanceId);
        var existingValue = await conn.QueryAsync<string>(
            @$"SELECT [Value] FROM {_tablePrefix}Arbitrator WHERE [Id]=@Id",
            new {Id = id}
        ).ToTaskAsync();

        if (existingValue.Count == 1)
            return existingValue.Single() == value;

        try
        {
            await conn.ExecuteAsync(@$"
                    INSERT INTO {_tablePrefix}Arbitrator ([Id], [Value])
                    VALUES (@Id, @Value);",
                new { Id = id, Value = value }
            );
        }
        catch (SqlException e)
        {
            if (e.Number != SqlError.UNIQUENESS_VIOLATION)
                throw;

            return await InnerPropose(groupId, instanceId, value);
        }

        return true;
    }
}
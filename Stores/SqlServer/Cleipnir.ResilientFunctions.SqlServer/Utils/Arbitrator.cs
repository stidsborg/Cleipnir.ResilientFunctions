using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
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
            var cmd = @$"            
                CREATE TABLE {_tablePrefix}Arbitrator (
                    [Id] NVARCHAR(255) PRIMARY KEY NOT NULL,                
                    [Value] NVARCHAR(255) NOT NULL
                )";

            await using var command = new SqlCommand(cmd, conn);
            await command.ExecuteNonQueryAsync();
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
            var sql = $"DROP TABLE {_tablePrefix}Arbitrator";
            await using var command = new SqlCommand(sql, conn);
            await command.ExecuteNonQueryAsync();
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
        {
            var sql = $"SELECT Value FROM {_tablePrefix}Arbitrator WHERE [Id]=@Id";
            await using var command = new SqlCommand(sql, conn);
            command.Parameters.AddWithValue("@Id", id);
            await using var reader = await command.ExecuteReaderAsync();
            while (reader.HasRows)
            {
                while (reader.Read())
                {
                    var fetchedValue = reader.GetString(0);
                    if (fetchedValue != null)
                        return value == fetchedValue;
                }

                reader.NextResult();
            }
        }
        
        try
        {
            var sql = $"INSERT INTO {_tablePrefix}Arbitrator ([Id], [Value]) VALUES (@Id, @Value)";
            await using var command = new SqlCommand(sql, conn);
            command.Parameters.AddWithValue("@Id", id);
            command.Parameters.AddWithValue("@Value", value);
            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException e)
        {
            if (e.Number != SqlError.UNIQUENESS_VIOLATION)
                throw;

            return await InnerPropose(groupId, instanceId, value);
        }

        return true;
    }

    public Task Delete(string groupId) => InnerDelete(groupId, instanceId: null);
    public Task Delete(string groupId, string instanceId) => InnerDelete(groupId, instanceId);

    private async Task InnerDelete(string groupId, string? instanceId)
    {
        await using var conn = await _connFunc();
        var id = KeyEncoder.Encode(groupId, instanceId);
        {
            var sql = $"DELETE FROM {_tablePrefix}Arbitrator WHERE [Id]=@Id";
            await using var command = new SqlCommand(sql, conn);
            command.Parameters.AddWithValue("@Id", id);
            await command.ExecuteNonQueryAsync();
        }
    }
}
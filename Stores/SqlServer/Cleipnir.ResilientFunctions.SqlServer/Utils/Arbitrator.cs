using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer.Utils;

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
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        try
        {
            var cmd = @$"            
                CREATE TABLE {_tablePrefix}Arbitrator (
                    [GroupName] NVARCHAR(255) NOT NULL,
                    [KeyId] NVARCHAR(255) NOT NULL,
                    [Value] NVARCHAR(255) NOT NULL,
                    PRIMARY KEY ([GroupName], [KeyId])
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
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}RFunctions_Arbitrator";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public Task<bool> Propose(string group, string key, string value)
        => InnerPropose(group, key, value);
    
    public Task<bool> Propose(string key, string value)
        => InnerPropose(group: "", key, value);
    
    private async Task<bool> InnerPropose(string group, string key, string value)
    {
        try
        {
            await using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            var sql = $"INSERT INTO {_tablePrefix}Arbitrator ([GroupName], [KeyId], [Value]) VALUES (@GroupName, @KeyId, @Value)";
            await using var command = new SqlCommand(sql, conn);
            command.Parameters.AddWithValue("@GroupName", group);
            command.Parameters.AddWithValue("@KeyId", key);
            command.Parameters.AddWithValue("@Value", value);
            await command.ExecuteNonQueryAsync();
            
            return true;
        }
        catch (SqlException e)
        {
            if (e.Number != SqlError.UNIQUENESS_VIOLATION)
                throw;

            var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();
            {
                var sql = $"SELECT Value FROM {_tablePrefix}Arbitrator WHERE [GroupName]=@GroupName AND [KeyId]=@KeyId";
                await using var command = new SqlCommand(sql, conn);
                command.Parameters.AddWithValue("@GroupName", group);
                command.Parameters.AddWithValue("@KeyId", key);
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

            throw new Exception($"Arbitrator {key}@{group} was not found");
        }
    }

    public Task Delete(string key) => InnerDelete(group: "", key);
    public Task Delete(string group, string key) => InnerDelete(group, key);

    private async Task InnerDelete(string group, string key)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        {
            var sql = $"DELETE FROM {_tablePrefix}Arbitrator WHERE [GroupName]=@GroupName AND [KeyId]=@KeyId";
            await using var command = new SqlCommand(sql, conn);
            command.Parameters.AddWithValue("@GroupName", group);
            command.Parameters.AddWithValue("@KeyId", key);
            await command.ExecuteNonQueryAsync();
        }
    }

    public async Task TruncateTable()
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        var sql = $"TRUNCATE TABLE {_tablePrefix}Arbitrator";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
}
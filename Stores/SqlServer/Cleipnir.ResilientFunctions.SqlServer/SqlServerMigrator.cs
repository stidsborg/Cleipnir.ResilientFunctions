using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerMigrator(string connectionString, string tablePrefix = "") : IMigrator
{
    public async Task<int?> Initialize(int version)
    {
        var atVersion = await InnerGetCurrentVersion();
        if (atVersion is not null)
            return atVersion.Value;
        
        await using var conn = await CreateConnection();
        var sql = @$"
            CREATE TABLE {tablePrefix}_schema (version INT NOT NULL);           
            INSERT INTO {tablePrefix}_schema (version) VALUES ({version});";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();

        return null;
    }

    public async Task SetVersion(int version)
    {
        await using var conn = await CreateConnection();
        var sql = @$"UPDATE {tablePrefix}_schema SET version = {version}";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<int?> InnerGetCurrentVersion()
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT TOP 1 version 
            FROM {tablePrefix}_schema";
        await using var command = new SqlCommand(sql, conn);
        try
        {
            var atVersion = (int?)await command.ExecuteScalarAsync();
            return atVersion;
        }
        catch (SqlException ex)
        {
            if (ex.Message.StartsWith("Invalid object name"))
                return null;

            throw;
        }
    }
    
    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
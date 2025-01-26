using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlMigrator(string connectionString, string tablePrefix = "") : IMigrator
{
    public async Task<int?> Initialize(int version)
    {
        var atVersion = await InnerGetCurrentVersion();
        if (atVersion is not null)
            return atVersion.Value;

        await using var conn = await CreateConnection();
        var sql = @$"
            CREATE TABLE IF NOT EXISTS {tablePrefix}_schema (version INT NOT NULL);
           
            INSERT INTO {tablePrefix}_schema (version)
                SELECT *
                FROM (SELECT {version} WHERE NOT EXISTS(SELECT 1 FROM {tablePrefix}_schema));";
        
        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
        
        return null;
    }

    public async Task SetVersion(int version)
    {
        await using var conn = await CreateConnection();
        var sql = $"UPDATE {tablePrefix}_schema SET version = {version}";
        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<int?> InnerGetCurrentVersion()
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT version 
            FROM {tablePrefix}_schema
            LIMIT 1";
        await using var command = new NpgsqlCommand(sql, conn);
        try
        {
            var atVersion = (int?)await command.ExecuteScalarAsync();
            return atVersion;
        }
        catch (NpgsqlException ex)
        {
            if (ex.SqlState == "42P01")
                return null;

            throw;
        }
    }
    
    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
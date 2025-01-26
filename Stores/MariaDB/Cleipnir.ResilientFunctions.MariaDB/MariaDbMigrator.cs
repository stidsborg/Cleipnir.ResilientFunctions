using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbMigrator(string connectionString, string tablePrefix = "") : IMigrator
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

        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();

        return null;
    }

    public async Task SetVersion(int version)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            UPDATE {tablePrefix}_schema 
            SET version = {version}            
            LIMIT 1";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<int?> InnerGetCurrentVersion()
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT version 
            FROM {tablePrefix}_schema
            LIMIT 1";
        await using var command = new MySqlCommand(sql, conn);
        try
        {
            var atVersion = (int?)await command.ExecuteScalarAsync();
            return atVersion;
        }
        catch (MySqlException ex)
        {
            if (ex.Message.StartsWith("Table") && ex.Message.EndsWith("doesn't exist"))
                return null;

            throw;
        }
    }
    
    private async Task<MySqlConnection> CreateConnection()
    {
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
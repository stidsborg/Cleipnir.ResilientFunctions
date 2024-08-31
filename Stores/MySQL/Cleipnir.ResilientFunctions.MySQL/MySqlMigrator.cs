using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MySQL;

public class MySqlMigrator(string connectionString, string tablePrefix = "") : IMigrator
{
    private const int CurrentVersion = 0;
    
    public async Task<bool> InitializeAndMigrate()
    {
        var atVersion = await GetCurrentVersion();
        if (atVersion == CurrentVersion)
            return false;

        if (atVersion is null)
        {
            await using var conn = await CreateConnection();
            var sql = @$"
            CREATE TABLE {tablePrefix}_schema (              
                version INT NOT NULL              
            );
           
            INSERT INTO {tablePrefix}_schema (version) 
            VALUES (1);
            ";
            await using var command = new MySqlCommand(sql, conn);
            await command.ExecuteNonQueryAsync();

            return true;
        }

        await Migrate(atVersion.Value);
        return false;
    }

    private async Task Migrate(int atVersion)
    {
        atVersion++;
        while (atVersion <= CurrentVersion)
        {
            await using var connection = await CreateConnection();
            await using var transaction = connection.BeginTransaction();
            var sql = Migrations[atVersion].Replace("TABLE_PREFIX", tablePrefix);

            await using var command = new MySqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();

            await SetCurrentVersion(atVersion, connection, transaction);
            await transaction.CommitAsync();
            
            atVersion++;
        }
    }

    private async Task<int?> GetCurrentVersion()
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
    
    private async Task SetCurrentVersion(int version, MySqlConnection connection, MySqlTransaction transaction)
    {
        var sql = @$"
            UPDATE {tablePrefix}_schema
            SET version = {version}";
        await using var command = new MySqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync();
    }
    
    private async Task<MySqlConnection> CreateConnection()
    {
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }

    private readonly Dictionary<int, string> Migrations = new();
    
    public async Task MigrateToLatestSchema()
    {
        var atVersion = await GetCurrentVersion();
        if (atVersion is null)
            return;

        await Migrate(atVersion.Value);
    }
}
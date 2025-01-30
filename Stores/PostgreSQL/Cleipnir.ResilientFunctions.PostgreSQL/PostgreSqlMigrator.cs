using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlMigrator(string connectionString, string tablePrefix = "") : IMigrator
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
            CREATE TABLE IF NOT EXISTS {tablePrefix}_schema (              
                version INT NOT NULL              
            );
           
            INSERT INTO {tablePrefix}_schema (version)
                SELECT *
                FROM (SELECT {CurrentVersion} WHERE NOT EXISTS(SELECT 1 FROM {tablePrefix}_schema));
            ";
            await using var command = new NpgsqlCommand(sql, conn);
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

            await using var command = new NpgsqlCommand(sql, connection);
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
    
    private async Task SetCurrentVersion(int version, NpgsqlConnection connection, NpgsqlTransaction transaction)
    {
        var sql = @$"
            UPDATE {tablePrefix}_schema
            SET version = {version}";
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync();
    }
    
    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
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
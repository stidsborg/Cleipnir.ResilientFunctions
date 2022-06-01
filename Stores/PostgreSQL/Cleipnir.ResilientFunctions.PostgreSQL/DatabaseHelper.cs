using System.Threading.Tasks;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public static class DatabaseHelper
{
    public static async Task CreateDatabaseIfNotExists(string connectionString)
    {
        var databaseName = Storage.DatabaseHelper.GetDatabaseName(connectionString);
        var connectionStringWithoutDatabase = Storage.DatabaseHelper.GetConnectionStringWithoutDatabase(connectionString);
        await using var conn = new NpgsqlConnection(connectionStringWithoutDatabase);
        await conn.OpenAsync();
        var sql = $"SELECT COUNT(*) FROM pg_database WHERE datname='{databaseName}'";
        await using (var existsCommand = new NpgsqlCommand(sql, conn))
        {
            var dbExists = (long) (await existsCommand.ExecuteScalarAsync() ?? 0);
            if (dbExists == 1) return;    
        }
        
        await using var createDbCommand = new NpgsqlCommand($"CREATE DATABASE {databaseName}", conn);
        await createDbCommand.ExecuteNonQueryAsync();
    }

    public static async Task RecreateDatabase(string connectionString)
    {
        var databaseName = Storage.DatabaseHelper.GetDatabaseName(connectionString);
        var connectionStringWithoutDatabase = Storage.DatabaseHelper.GetConnectionStringWithoutDatabase(connectionString);
        await using var conn = new NpgsqlConnection(connectionStringWithoutDatabase);
        await conn.OpenAsync();
        var sql = $"SELECT COUNT(*) FROM pg_database WHERE datname='{databaseName}'";
        await using (var existsCommand = new NpgsqlCommand(sql, conn))
        {
            var dbExists = (long) (await existsCommand.ExecuteScalarAsync() ?? 0);
            if (dbExists == 1)
            {
                await using var dropDbCommand = new NpgsqlCommand($"DROP DATABASE {databaseName}", conn);
                await dropDbCommand.ExecuteNonQueryAsync();
            }
        }
        
        await using var createDbCommand = new NpgsqlCommand($"CREATE DATABASE {databaseName}", conn);
        await createDbCommand.ExecuteNonQueryAsync();
    }
}
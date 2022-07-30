using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public static class DatabaseHelper
{
    public static async Task RecreateDatabase(string connectionString)
    {
        var connectionStringWithDatabaseName = Storage.DatabaseHelper.GetConnectionStringWithoutDatabase(connectionString);
        var databaseName = Storage.DatabaseHelper.GetDatabaseName(connectionString);
        await using var conn = new SqlConnection(connectionStringWithDatabaseName);
        await conn.OpenAsync();
        var sql = $@"
            IF EXISTS(SELECT * FROM sys.databases WHERE name = '{databaseName}')
            BEGIN
                DROP DATABASE [{databaseName}]
            END";

        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();

        await CreateDatabaseIfNotExists(connectionString);
    }
    
    public static async Task CreateDatabaseIfNotExists(string connectionString)
    {
        var connectionStringWithDatabaseName = Storage.DatabaseHelper.GetConnectionStringWithoutDatabase(connectionString);
        var databaseName = Storage.DatabaseHelper.GetDatabaseName(connectionString);
        await using var conn = new SqlConnection(connectionStringWithDatabaseName);
        await conn.OpenAsync();
        var sql = $@"
            IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '{databaseName}')
            BEGIN
                CREATE DATABASE [{databaseName}]
            END";

        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
}
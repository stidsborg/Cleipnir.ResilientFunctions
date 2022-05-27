using MySql.Data.MySqlClient;

namespace Cleipnir.ResilientFunctions.MySQL;

public static class DatabaseHelper
{
    public static async Task CreateDatabaseIfNotExists(string connectionString)
    {
        var databaseName = Storage.DatabaseHelper.GetDatabaseName(connectionString);
        var connectionStringWithoutDatabaseName = Storage.DatabaseHelper.GetConnectionStringWithoutDatabase(connectionString);
        var conn = new MySqlConnection(connectionStringWithoutDatabaseName);
        await conn.OpenAsync();

        await using var createDbCommand = new MySqlCommand($"CREATE DATABASE IF NOT EXISTS {databaseName}", conn);
        await createDbCommand.ExecuteNonQueryAsync();
    }

    public static async Task<MySqlConnection> CreateOpenConnection(string connectionString)
    {
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
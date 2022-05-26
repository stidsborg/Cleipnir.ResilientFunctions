using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;

namespace Cleipnir.ResilientFunctions.MySQL;

public static class DatabaseHelper
{
    public static async Task CreateDatabaseIfNotExists(string connectionString)
    {
        var regEx = new Regex("Database=(?<DatabaseName>[a-zA-Z0-9_]+);", RegexOptions.IgnoreCase);
        var match = regEx.Match(connectionString);
        if (!match.Success)
            throw new ArgumentException("Could not find database name in connection string", nameof(connectionString));

        var connectionStringWithDatabaseName = connectionString.Replace(match.Value, "");
        var conn = new MySqlConnection(connectionStringWithDatabaseName);
        var databaseName = match.Groups["DatabaseName"].Value;
        await conn.OpenAsync();

        await using var createDbCommand = new MySqlCommand($"CREATE DATABASE IF NOT EXISTS {databaseName}", conn);
        await createDbCommand.ExecuteNonQueryAsync();
    }

    public static string GetDatabaseName(string connectionString)
    {
        var regEx = new Regex("Database=(?<DatabaseName>[a-zA-Z0-9_]+);", RegexOptions.IgnoreCase);
        var match = regEx.Match(connectionString);
        if (!match.Success)
            throw new ArgumentException("Could not find database name in connection string", nameof(connectionString));

        var databaseName = match.Groups["DatabaseName"].Value;
        return databaseName;
    }
    
    public static string GetConnectionStringWithoutDatabase(string connectionString)
    {
        var regEx = new Regex("Database=(?<DatabaseName>[a-zA-Z0-9_]+);", RegexOptions.IgnoreCase);
        var match = regEx.Match(connectionString);
        if (!match.Success)
            throw new ArgumentException("Could not find database name in connection string", nameof(connectionString));

        var connectionStringWithoutDatabase = regEx.Replace(connectionString, "");
        return connectionStringWithoutDatabase;
    }
    
    public static async Task<MySqlConnection> CreateOpenConnection(string connectionString)
    {
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
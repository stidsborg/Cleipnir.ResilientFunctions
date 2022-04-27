using System.Text.RegularExpressions;
using Dapper;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public static class DatabaseHelper
{
    public static async Task CreateDatabaseIfNotExists(string connectionString)
    {
        var regEx = new Regex("Database=(?<DatabaseName>[a-zA-Z0-9_]+);", RegexOptions.IgnoreCase);
        var match = regEx.Match(connectionString);
        if (!match.Success)
            throw new ArgumentException("Could not find database name in connection string", nameof(connectionString));

        var connectionStringWithDatabaseName = connectionString.Replace(match.Value, "");
        var conn = new NpgsqlConnection(connectionStringWithDatabaseName);
        var databaseName = match.Groups["DatabaseName"].Value;
        await conn.OpenAsync();
        var dbExistsRow = await conn.QueryAsync<int>($"SELECT 1 FROM pg_database WHERE datname='{databaseName}'");
        if (dbExistsRow.Any()) return;
            
        await conn.ExecuteAsync($"CREATE DATABASE {databaseName}");
    }
}
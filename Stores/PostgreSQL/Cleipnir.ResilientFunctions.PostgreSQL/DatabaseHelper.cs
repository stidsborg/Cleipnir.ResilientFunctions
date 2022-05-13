using System;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
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

        var sql = $"SELECT COUNT(*) FROM pg_database WHERE datname='{databaseName}'";
        await using (var existsCommand = new NpgsqlCommand(sql, conn))
        {
            var dbExists = (long) (await existsCommand.ExecuteScalarAsync() ?? 0);
            if (dbExists == 1) return;    
        }
        
        await using var createDbCommand = new NpgsqlCommand($"CREATE DATABASE {databaseName}", conn);
        await createDbCommand.ExecuteNonQueryAsync();
    }
}
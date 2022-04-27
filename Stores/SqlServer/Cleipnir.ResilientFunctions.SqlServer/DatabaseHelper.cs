using System;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public static class DatabaseHelper
{
    public static async Task CreateDatabaseIfNotExists(string connectionString)
    {
        var regEx = new Regex("Database=(?<DatabaseName>[a-zA-Z0-9_]+);", RegexOptions.IgnoreCase);
        var match = regEx.Match(connectionString);
        if (!match.Success)
            throw new ArgumentException("Could not find database name in connection string", nameof(connectionString));

        var connectionStringWithDatabaseName = connectionString.Replace(match.Value, "");
        var conn = new SqlConnection(connectionStringWithDatabaseName);
        var databaseName = match.Groups["DatabaseName"].Value;
        await conn.OpenAsync();
        await conn.ExecuteAsync($@"
            IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '{databaseName}')
            BEGIN
                CREATE DATABASE [{databaseName}]
            END"
        );
    }
}
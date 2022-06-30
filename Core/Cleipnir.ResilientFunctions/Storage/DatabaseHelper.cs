using System;
using System.Text.RegularExpressions;

namespace Cleipnir.ResilientFunctions.Storage;


// TODO: Move to stores. Rename to SQL Database Helper or similar. This is not for all databases.
public static class DatabaseHelper
{
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
}
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;

namespace EnsureDatabaseConnections;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var databases = args.Length > 0
            ? args.Select(a => a.ToLowerInvariant()).ToHashSet()
            : new HashSet<string> { "mariadb", "postgres", "sqlserver" };

        var checks = new List<(string Name, Func<Task> Connect)>();
        if (databases.Contains("mariadb"))
            checks.Add(("MariaDB", CreateAndOpenMariaDbConnection));
        if (databases.Contains("postgres"))
            checks.Add(("Postgres", CreateAndOpenPostgresConnection));
        if (databases.Contains("sqlserver"))
            checks.Add(("SQL Server", CreateAndOpenSqlServerConnection));

        foreach (var (name, connect) in checks)
        {
            var retry = 1;
            while (true)
            {
                await Task.Delay(1_000);
                Console.WriteLine($"[{name}] Trying {retry}/20");
                try
                {
                    await connect();
                    break;
                }
                catch (Exception e)
                {
                    if (retry == 20)
                    {
                        Console.WriteLine($"Unable to connect to {name}. Exception: {e}");
                        return -1;
                    }
                    retry++;
                }
            }
        }

        Console.WriteLine("All connections were established successfully");
        return 0;
    }
    
    private static async Task CreateAndOpenMariaDbConnection()
    {
        const string connectionString = "server=localhost;userid=root;password=Pa55word!;AllowPublicKeyRetrieval=True;";
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
    }

    private static async Task CreateAndOpenPostgresConnection()
    {
        const string connectionString = "Server=localhost;User Id=postgres;Password=Pa55word!;Include Error Detail=true;";
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
    }

    private static async Task CreateAndOpenSqlServerConnection()
    {
        const string connectionString = "Server=localhost;User Id=sa;Password=Pa55word!;Encrypt=True;TrustServerCertificate=True;";
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
    }
}
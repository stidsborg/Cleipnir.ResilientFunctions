using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;

namespace EnsureDatabaseConnections;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var retry = 1;
        while (true)
        {
            await Task.Delay(1_000);
            Console.WriteLine($"Trying {retry}/20");
            
            try
            {
                await CreateAndOpenMariaDbConnection();
            }
            catch (Exception e)
            {
                if (retry == 20)
                {
                    Console.WriteLine($"Unable to connect to MariaDB. Exception: {e}");
                    return -1;
                }
                retry++;
                continue;
            }
            
            try
            {
                await CreateAndOpenPostgresConnection();
            }
            catch (Exception e)
            {
                if (retry == 20)
                {
                    Console.WriteLine($"Unable to connect to Postgres. Exception: {e}");
                    return -1;
                }
                retry++;
                continue;
            }
            
            try
            {
                await CreateAndOpenSqlServerConnection();
            }
            catch (Exception e)
            {
                if (retry == 20)
                {
                    Console.WriteLine($"Unable to connect to SQL Server. Exception: {e}");
                    return -1;
                }
                retry++;
                continue;
            }
            
            break;
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
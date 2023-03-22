using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;

namespace EnsureDatabaseConnections;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var failed = true;
        for (var i = 0; i < 10; i++)
        {
            await Task.Delay(1_000);
            
            try
            {
                await CreateAndOpenMySqlConnection();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Unable to connect to MySQL. Exception: {e}");
                continue;
            }
            
            try
            {
                await CreateAndOpenPostgresConnection();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Unable to connect to PostgresSQL. Exception: {e}");
                continue;
            }
            
            try
            {
                await CreateAndOpenSqlServerConnection();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Unable to connect to PostgresSQL. Exception: {e}");
                continue;
            }

            failed = false;
            break;
        }

        if (failed)
        {
            Console.WriteLine("Unable to connect to all databases");
            return -1;
        } 
            
        Console.WriteLine("All connections were established successfully");
        return 0;
    }
    
    private static async Task CreateAndOpenMySqlConnection()
    {
        const string connectionString = "server=localhost;userid=root;password=Pa55word!;SSL Mode=None";
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
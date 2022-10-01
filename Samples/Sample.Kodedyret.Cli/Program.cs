using System.Text;
using Npgsql;

namespace Sample.Kodedyret.Cli;

public static class Program
{
    public static async Task Main(string[] args)
    {
        if (args.Length < 1 || args[0].ToLower() is not ("postneworder" or "cleardatabase"))
        {
            Console.WriteLine("Usage: PostNewOrder");
            Console.WriteLine("       ClearDatabase");
            return;
        }
        
        if (args[0].ToLower() == "postneworder")
            await PostNewOrder();
        else if (args[0].ToLower() == "cleardatabase") 
            await ClearDatabase();
    }

    private static async Task PostNewOrder()
    {
        var httpClient = new HttpClient();
        var json = @"
        {
            ""orderId"": ""MK-12321"",
            ""customerId"": ""3fa85f64-5717-4562-b3fc-2c963f66afa6"",
            ""productIds"": [
                ""03f9bbfa-67de-41de-aec1-7abe56784b44"",
                ""9c8e8be9-f742-4635-a477-c333e8d7cb89"",
                ""d491d0ed-a357-438c-9ec7-b9b1ab7c5f85""
            ],
            ""totalPrice"": 125.98
        }";
        var response = await httpClient.PostAsync(
            "http://localhost:5000/order",
            new StringContent(json, Encoding.UTF8, "application/json")
        );
        response.EnsureSuccessStatusCode();
    }

    private static async Task ClearDatabase()
    {
        const string connectionString = "Server=localhost;Port=5432;Userid=postgres;Password=Pa55word!;Database=kodedyret;";
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            @"
            DO
            $do$
            BEGIN
                IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = 'rfunctions')
                THEN TRUNCATE TABLE rfunctions;
            END IF;
            END
            $do$",
            conn
        );
        await cmd.ExecuteNonQueryAsync();
    }
}
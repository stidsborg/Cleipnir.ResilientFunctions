using System.Text.Json;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests;

namespace Cleipnir.ResilientFunctions.StressTests
{
    internal static class Program
    {
        private static async Task<int> Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Usage: StressTests <all, mysql, postgres, sqlserver>");
                Console.WriteLine("       StressTests recreate_dbs");
                return 1;
            }

            if (args[0].ToLower() == "recreate_dbs")
            {
                Console.WriteLine("Recreating Databases:");
                Console.WriteLine("MySQL...");
                await new MySqlEngine().RecreateDatabase();
                Console.WriteLine("Postgres...");
                await new PostgreSqlEngine().RecreateDatabase();
                Console.WriteLine("SQL Server...");
                await new SqlServerEngine().RecreateDatabase();
                return 0;
            }
            
            var engines = new List<IEngine>();

            if (args.Any(arg => arg.ToLower() == "all"))
                args = new[] { "mysql", "postgres", "sqlserver" };
            
            foreach (var arg in args)
            {
                if (arg == "mysql")
                    engines.Add(new MySqlEngine());
                if (arg == "postgres")
                    engines.Add(new PostgreSqlEngine());
                if (arg == "sqlserver")
                    engines.Add(new SqlServerEngine());                
            }
            
            if (engines.Count == 0) 
            {
                Console.WriteLine("Usage: StressTests <all, mysql, postgres, sqlserver>");
                return 1;
            }
                
            var testResults = new Dictionary<string, Dictionary<string, TestResult>>();

            foreach (var engine in engines)
            {
                Console.WriteLine("*************************************************************");
                Console.WriteLine($"* {engine.GetType().Name.ToUpper()} *");
                Console.WriteLine("*************************************************************");
                var engineTestResults = new Dictionary<string, TestResult>();
                testResults[engine.GetType().Name] = engineTestResults;
                Console.WriteLine();
                Console.WriteLine(nameof(CrashedTest).ToUpper()); 
                engineTestResults[nameof(CrashedTest)] = await CrashedTest.Perform(engine);;
                Console.WriteLine();
                Console.WriteLine(nameof(PostponedTest).ToUpper());
                engineTestResults[nameof(PostponedTest)] = await PostponedTest.Perform(engine);
                Console.WriteLine();
                Console.WriteLine(nameof(MixingTest).ToUpper());
                engineTestResults[nameof(MixingTest)] = await MixingTest.Perform(engine);
                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine("RESULTS: ");
            Console.WriteLine(JsonSerializer.Serialize(testResults, new JsonSerializerOptions { WriteIndented = true }));

            return 0;
        }
    }
}
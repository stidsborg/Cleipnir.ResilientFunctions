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
                Console.WriteLine("Usage: StressTests <all, mariadb, postgres, sqlserver>");
                Console.WriteLine("       StressTests recreate_dbs");
                Console.WriteLine("       StressTests long_running <mariadb, postgres, sqlserver>");
                return 1;
            }

            if (args[0].ToLower() == "recreate_dbs")
            {
                Console.WriteLine("Recreating Databases:");
                Console.WriteLine("MariaDB...");
                await new MariaDBEngine().RecreateDatabase();
                Console.WriteLine("Postgres...");
                await new PostgreSqlEngine().RecreateDatabase();
                Console.WriteLine("SQL Server...");
                await new SqlServerEngine().RecreateDatabase();
                return 0;
            }

            if (args[0].ToLower() == "long_running")
            {
                IEngine? engine = args[1].ToLower() switch
                {
                    "mariadb" => new MariaDBEngine(),
                    "postgres" => new PostgreSqlEngine(),
                    "sqlserver" => new SqlServerEngine(),
                    _ => null
                };
                if (engine == null)
                {
                    Console.WriteLine("Usage: StressTests long_running <mariadb, postgres, sqlserver>");
                    return 1;
                }

                Console.WriteLine("Press enter to stop test");
                await LongRunningStressfulTest.Perform(engine);
                return 0;
            }
            
            var engines = new List<IEngine>();

            if (args.Any(arg => arg.ToLower() == "all"))
                args = ["mariadb", "postgres", "sqlserver"];
            
            foreach (var arg in args)
            {
                if (arg == "mariadb")
                    engines.Add(new MariaDBEngine());
                if (arg == "postgres")
                    engines.Add(new PostgreSqlEngine());
                if (arg == "sqlserver")
                    engines.Add(new SqlServerEngine());                
            }
            
            if (engines.Count == 0) 
            {
                Console.WriteLine("Usage: StressTests <all, mariadb, postgres, sqlserver>");
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
                Console.WriteLine(nameof(ChildWorkflowsTest).ToUpper());
                engineTestResults[nameof(ChildWorkflowsTest)] = await ChildWorkflowsTest.Perform(engine);
                Console.WriteLine();
                Console.WriteLine(nameof(SuspensionTest).ToUpper());
                engineTestResults[nameof(SuspensionTest)] = await SuspensionTest.Perform(engine);
                Console.WriteLine();
                Console.WriteLine(nameof(RestartSuspensionTest).ToUpper());
                engineTestResults[nameof(RestartSuspensionTest)] = await RestartSuspensionTest.Perform(engine);
                Console.WriteLine();
                Console.WriteLine(nameof(DirectInvocationTest).ToUpper());
                engineTestResults[nameof(DirectInvocationTest)] = await DirectInvocationTest.Perform(engine);
                Console.WriteLine();
                Console.WriteLine(nameof(BulkInsertionTest).ToUpper());
                engineTestResults[nameof(BulkInsertionTest)] = await BulkInsertionTest.Perform(engine);
            }

            Console.WriteLine();
            Console.WriteLine("RESULTS: ");
            Console.WriteLine(JsonSerializer.Serialize(testResults, new JsonSerializerOptions { WriteIndented = true }));

            return 0;
        }
    }
}
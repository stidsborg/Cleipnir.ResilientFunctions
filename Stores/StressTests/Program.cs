using System.Text.Json;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests;

namespace Cleipnir.ResilientFunctions.StressTests
{
    internal static class Program
    {
        private static async Task Main()
        {
            var testResults = new Dictionary<string, Dictionary<string, int>>();
            var engines = new IEngine[]
            {
                new MongoDbEngine(),
                new MySqlEngine(),
                new PostgreSqlEngine(),
                new SqlServerEngine()
            };

            foreach (var engine in engines)
            {
                Console.WriteLine("*************************************************************");
                Console.WriteLine($"* {engine.GetType().Name.ToUpper()} *");
                Console.WriteLine("*************************************************************");
                var engineTestResults = new Dictionary<string, int>();
                testResults[engine.GetType().Name] = engineTestResults;
                Console.WriteLine();
                Console.WriteLine(nameof(CrashedTest).ToUpper()); 
                engineTestResults[nameof(CrashedTest)] = await CrashedTest.Perform(engine);;
                Console.WriteLine();
                Console.WriteLine(nameof(PostponedTest).ToUpper());
                engineTestResults[nameof(PostponedTest)] = await PostponedTest.Perform(engine);
                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine("RESULTS: ");
            Console.WriteLine(JsonSerializer.Serialize(testResults, new JsonSerializerOptions { WriteIndented = true }));
        }
    }
}
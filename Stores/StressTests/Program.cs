using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests;

namespace Cleipnir.ResilientFunctions.StressTests
{
    internal static class Program
    {
        private static async Task Main()
        {
            var engines = new IEngine[]
            {
                new MongoDbEngine(),
                new MySqlEngine(),
                new PostgreSqlEngine(),
                new SqlServerEngine()
            };

            foreach (var engine in engines)
            {
                Console.WriteLine(engine.GetType().Name);
                await CrashedTest.Perform(engine);
                Console.WriteLine();
                await PostponedTest.Perform(engine);
                Console.WriteLine();
            }
        }
    }
}
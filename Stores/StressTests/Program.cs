using Cleipnir.ResilientFunctions.MySQL.StressTest;
using Cleipnir.ResilientFunctions.PostgreSQL.StressTest;
using Cleipnir.ResilientFunctions.SqlServer.StressTest;
using Cleipnir.ResilientFunctions.StressTests.Common;
using Cleipnir.ResilientFunctions.StressTests.Common.Engines;

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
using System.Data.SqlClient;
using System.Runtime.CompilerServices;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Cleipnir.ResilientFunctions.Messaging.Tests.Utils;

namespace Cleipnir.ResilientFunctions.Messaging.SqlServer.Tests
{
    [TestClass]
    public static class Sql
    {
        private static string ConnectionString { get; }

        static Sql()
        {
            ConnectionString = 
                Environment.GetEnvironmentVariable("Cleipnir.RFunctions.SqlServer.Tests.ConnectionString")
                ?? "Server=localhost;Database=rfunctions;User Id=sa;Password=Pa55word!;Encrypt=True;TrustServerCertificate=True;";
        }

        [AssemblyInitialize]
        public static void AssemblyInit(TestContext testContext)
        {
            var connectionStringWithoutDatabase = DatabaseHelper.GetConnectionStringWithoutDatabase(ConnectionString);
            var databaseName = DatabaseHelper.GetDatabaseName(ConnectionString);
            
            using var conn = new SqlConnection(connectionStringWithoutDatabase);
            conn.Open();
            {
                using var command = new SqlCommand($"DROP DATABASE IF EXISTS {databaseName}", conn);
                command.ExecuteNonQuery();    
            }
            {
                using var command = new SqlCommand($"CREATE DATABASE {databaseName}", conn);
                command.ExecuteNonQuery();    
            }
        }

        public static async Task<IEventStore> CreateAndInitializeEventStore(
            [CallerFilePath] string sourceFilePath = "",
            [CallerMemberName] string callMemberName = "")
        {
            var sourceFileName = sourceFilePath
                .Split(new[] {"\\", "/"}, StringSplitOptions.None)
                .Last()
                .Replace(".cs", "");
            var store = new SqlServerEventStore(ConnectionString, $"{sourceFileName}_{callMemberName}");
            await store.DropUnderlyingTable();
            await store.Initialize();
            return store;
        }
            
    }
}
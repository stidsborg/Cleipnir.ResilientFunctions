using System.Runtime.CompilerServices;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Cleipnir.ResilientFunctions.Messaging.Tests.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.Messaging.PostgreSQL.Tests
{
    [TestClass]
    public static class Sql
    {
        private static string ConnectionString { get; }

        static Sql()
        {
            ConnectionString = 
                Environment.GetEnvironmentVariable("Cleipnir.RFunctions.PostgreSQL.Tests.ConnectionString")
                ?? "Server=localhost;Database=rfunctions;User Id=postgres;Password=Pa55word!";
        }

        [AssemblyInitialize]
        public static void AssemblyInit(TestContext testContext)
        {
            var connectionStringWithoutDatabase = DatabaseHelper.GetConnectionStringWithoutDatabase(ConnectionString);
            var databaseName = DatabaseHelper.GetDatabaseName(ConnectionString);
            
            using var conn = new NpgsqlConnection(connectionStringWithoutDatabase);
            conn.Open();
            {
                using var command = new NpgsqlCommand($"DROP DATABASE IF EXISTS {databaseName}", conn);
                command.ExecuteNonQuery();    
            }
            {
                using var command = new NpgsqlCommand($"CREATE DATABASE {databaseName}", conn);
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
            var store = new PostgreSqlEventStore(ConnectionString, $"{sourceFileName}_{callMemberName}");
            await store.DropUnderlyingTable();
            await store.Initialize();
            return store;
        }
            
    }
}
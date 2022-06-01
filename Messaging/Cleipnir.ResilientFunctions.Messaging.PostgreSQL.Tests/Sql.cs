using Cleipnir.ResilientFunctions.PostgreSQL;
using Npgsql;

namespace Cleipnir.ResilientFunctions.Messaging.PostgreSQL.Tests
{
    [TestClass]
    public static class Sql
    {
        private static string ConnectionString { get; }
        public static Func<Task<NpgsqlConnection>> ConnFunc { get; set; }
        
        static Sql()
        {
            ConnectionString = 
                Environment.GetEnvironmentVariable("Cleipnir.RFunctions.PostgreSQL.Tests.ConnectionString")
                ?? "Server=localhost;Database=rfunctions;User Id=postgres;Password=Pa55word!";
            ConnFunc = async () =>
            {
                var conn = new NpgsqlConnection(ConnectionString);
                await conn.OpenAsync();
                return conn;
            };
        }

        [AssemblyInitialize]
        public static void AssemblyInit(TestContext testContext)
        {
            var connectionStringWithoutDatabase = Storage.DatabaseHelper.GetConnectionStringWithoutDatabase(ConnectionString);
            var databaseName = Storage.DatabaseHelper.GetDatabaseName(ConnectionString);
            
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

        public static async Task<PostgreSqlFunctionStore> CreateAndInitializeStore(string testClass, string testMethod)
        {
            var store = new PostgreSqlFunctionStore(ConnectionString); 
            await store.Initialize();
            await store.TruncateTable();
            return store;
        }

        public static async Task<PostgreSqlEventStore> CreateAndInitializeEventStore(string testClass, string testMethod)
        {
            var store = new PostgreSqlEventStore(ConnectionString);
            await store.DropUnderlyingTable();
            await store.Initialize();
            return store;
        }
            
    }
}
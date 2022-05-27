using System.Runtime.CompilerServices;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySql.Data.MySqlClient;

namespace Cleipnir.ResilientFunctions.MySQL.Tests
{
    [TestClass]
    public static class Sql
    {
        public static string ConnectionString { get; }
        public static Func<Task<MySqlConnection>> ConnFunc { get; set; }
        
        static Sql()
        {
            ConnectionString = 
                Environment.GetEnvironmentVariable("Cleipnir.RFunctions.MySQL.Tests.ConnectionString")
                ?? @"server=localhost;userid=root;password=Pa55word!;database=rfunctions_tests;";
            ConnFunc = async () =>
            {
                var conn = new MySqlConnection(ConnectionString);
                await conn.OpenAsync();
                return conn;
            };
        }

        [AssemblyInitialize]
        public static void AssemblyInit(TestContext testContext)
        {
            // DROP test database if exists and create it again
            var database = Storage.DatabaseHelper.GetDatabaseName(ConnectionString);

            var connectionStringWithoutDatabase = Storage.DatabaseHelper.GetConnectionStringWithoutDatabase(ConnectionString);

            using var conn = new MySqlConnection(connectionStringWithoutDatabase);
            conn.Open();
            {
                using var command = new MySqlCommand($"DROP DATABASE IF EXISTS {database}", conn);
                command.ExecuteNonQuery();    
            }
            {
                using var command = new MySqlCommand($"CREATE DATABASE {database}", conn);
                command.ExecuteNonQuery();    
            }
        }

        private static async Task<MySqlFunctionStore> CreateAndInitializeStore(string testClass, string testMethod)
        {
            var store = new MySqlFunctionStore(ConnectionString); 
            await store.Initialize();
            await store.TruncateTable();
            return store;
        }

        public static Task<IFunctionStore> AutoCreateAndInitializeStore(
            [CallerFilePath] string sourceFilePath = "",
            [CallerMemberName] string callMemberName = ""
        )
        {
            var sourceFileName = sourceFilePath
                .Split(new[] {"\\", "/"}, StringSplitOptions.None)
                .Last()
                .Replace(".cs", "")
                .Replace(".", "_");

            return CreateAndInitializeStore(sourceFileName, callMemberName)
                .Map(store => (IFunctionStore) store);
        }
    }
}
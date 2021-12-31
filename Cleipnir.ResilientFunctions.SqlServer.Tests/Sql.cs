using System;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests
{
    [TestClass]
    public static class Sql
    {
        public static Func<Task<SqlConnection>> ConnFunc { get; }
        public static string ConnectionString { get; }

        static Sql()
        {
            ConnectionString = 
                Environment.GetEnvironmentVariable("Cleipnir.RFunctions.SqlServer.Tests.ConnectionString")
                ?? "Server=localhost;Database=rfunctions;User Id=sa;Password=Pa55word!";

            ConnFunc = async () =>
            {
                var conn = new SqlConnection(ConnectionString);
                await conn.OpenAsync();
                return conn;
            };
        }
        
        [AssemblyInitialize]
        public static void AssemblyInit(TestContext testContext)
        {
            // DROP test database if exists and create it again
            var database = ConnectionString
                .Split("Database=")[1]
                .Split(";")
                .First();
            
            var connectionStringWithoutDatabase = ConnectionString
                .Split($"Database={database};")
                .Aggregate("", string.Concat);

            using var conn = new SqlConnection(connectionStringWithoutDatabase);
            conn.Execute($"DROP DATABASE IF EXISTS {database}");
            conn.Execute($"CREATE DATABASE {database}");
        }
        
        public static async Task<SqlServerFunctionStore> CreateAndInitializeStore(string testClass, string testMethod)
        {
            var store = new SqlServerFunctionStore(ConnFunc, $"{testClass}_{testMethod}");
            await store.Initialize();
            return store;
        }

        public static SqlServerFunctionStore AutoCreateAndInitializeStore(
            [System.Runtime.CompilerServices.CallerFilePath] string sourceFilePath = "",
            [System.Runtime.CompilerServices.CallerMemberName] string callMemberName = ""
        )
        {
            var sourceFileName = sourceFilePath
                .Split(new[] {"\\", "/"}, StringSplitOptions.None)
                .Last()
                .Replace(".cs", "");

            return CreateAndInitializeStore(sourceFileName, callMemberName).Result;
        }
    }
}
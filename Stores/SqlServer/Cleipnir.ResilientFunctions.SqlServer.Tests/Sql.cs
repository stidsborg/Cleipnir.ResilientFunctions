using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests
{
    [TestClass]
    public static class Sql
    {
        public static string ConnectionString { get; }

        static Sql()
        {
            ConnectionString = 
                Environment.GetEnvironmentVariable("Cleipnir.RFunctions.SqlServer.Tests.ConnectionString")
                ?? "Server=localhost;Database=rfunctions;User Id=sa;Password=Pa55word!;Encrypt=True;TrustServerCertificate=True;";
        }
        
        [AssemblyInitialize]
        public static void AssemblyInit(TestContext testContext)
        {
            var connectionStringWithoutDatabase = Storage.DatabaseHelper.GetConnectionStringWithoutDatabase(ConnectionString);
            var databaseName = Storage.DatabaseHelper.GetDatabaseName(ConnectionString);

            using var conn = new SqlConnection(connectionStringWithoutDatabase);
            conn.Execute($"DROP DATABASE IF EXISTS {databaseName}");
            conn.Execute($"CREATE DATABASE {databaseName}");
        }

        public static async Task<SqlServerFunctionStore> CreateAndInitializeStore(string testClass, string testMethod)
        {
            var store = new SqlServerFunctionStore(ConnectionString, $"{testClass}_{testMethod}");
            await store.Initialize();
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
                .Replace(".cs", "");

            return CreateAndInitializeStore(sourceFileName, callMemberName)
                .Map(store => (IFunctionStore) store);
        }
    }
}
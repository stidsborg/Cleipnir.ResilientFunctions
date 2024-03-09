using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
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
            conn.Open();
            
            Execute($"DROP DATABASE IF EXISTS {databaseName}", conn);
            Execute($"CREATE DATABASE {databaseName}", conn);
        }

        private static async Task<SqlServerFunctionStore> CreateAndInitializeStore(string testClass, string testMethod)
        {
            var store = new SqlServerFunctionStore(ConnectionString, tablePrefix: ComputeSha256Hash(testClass + "§" + testMethod));
            await store.DropIfExists();
            await store.Initialize();
            return store;
        }
        
        private static string ComputeSha256Hash(string rawData)
        {
            // Create a SHA256
            using SHA256 sha256Hash = SHA256.Create();
            // ComputeHash - returns byte array
            var bytes = sha256Hash.ComputeHash(Encoding.UTF8.GetBytes(rawData));

            // Convert byte array to a string
            var builder = new StringBuilder();
            foreach (var t in bytes)
                builder.Append(ReplaceDigits(t.ToString("X")));
            
            return builder.ToString();
        }

        private static string ReplaceDigits(string s) 
            => s.Replace('0', 'a')
                .Replace('1', 'b')
                .Replace('2', 'c')
                .Replace('3', 'd')
                .Replace('4', 'e')
                .Replace('5', 'f')
                .Replace('6', 'g')
                .Replace('7', 'h')
                .Replace('8', 'i')
                .Replace('9', 'j');

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

        private static void Execute(string sql, SqlConnection connection)
        {
            var command = new SqlCommand(sql, connection);
            command.ExecuteNonQuery();
        }
    }
}
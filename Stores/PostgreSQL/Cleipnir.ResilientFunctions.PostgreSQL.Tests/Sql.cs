using System;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests
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

        private static async Task<PostgreSqlFunctionStore> CreateAndInitializeStore(string testClass, string testMethod)
        {
            var store = new PostgreSqlFunctionStore(ConnectionString); 
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
                .Replace(".cs", "");

            return CreateAndInitializeStore(sourceFileName, callMemberName)
                .Map(store => (IFunctionStore) store);
        }
    }
}
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
            //todo if postgresqlconnection found use it instead!
            
            var password = Environment.GetEnvironmentVariable("POSTGRESQLPASSWORD");
            if (password == null)
                throw new InvalidOperationException("Environment variable 'POSTGRESQLPASSWORD' not found");
            if (string.IsNullOrEmpty(password))
                throw new InvalidOperationException("Environment variable 'POSTGRESQLPASSWORD' is the empty string");
            ConnectionString = $"Server=abul.db.elephantsql.com;Port=5432;Userid=utdbwvkk;Password={password};Database=utdbwvkk;Maximum Pool Size=1";
        }
        
        public static async Task<PostgreSqlFunctionStore> CreateAndInitializeStore(string testClass, string testMethod)
        {
            var store = new PostgreSqlFunctionStore(ConnectionString); //$"{testClass}_{testMethod}_"
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
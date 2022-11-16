using System.Runtime.CompilerServices;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests
{
    [TestClass]
    public static class NoSql
    {
        private static string ConnectionString { get; }

        static NoSql()
        {
            ConnectionString = 
                Environment.GetEnvironmentVariable("Cleipnir.RFunctions.MongoDB.Tests.ConnectionString")
                ?? "mongodb://root:Pa55word!@localhost:27017?authSource=admin";
        }

        private static async Task<MongoDbFunctionStore> CreateAndInitializeStore(string testClass, string testMethod)
        {
            var store = new MongoDbFunctionStore(
                ConnectionString, 
                databaseName: "rfunctions_tests",
                collectionName: $"{testClass}_{testMethod}"
            ); 
            await store.Initialize();
            await store.DropUnderlyingCollection();
            return store;
        }
        
        private static async Task<IEventStore> CreateAndInitializeEventStore(string testClass, string testMethod)
        {
            var store = new MongoDbEventStore(
                ConnectionString, 
                databaseName: "rfunctions_tests",
                collectionName: $"{testClass}_{testMethod}"
            ); 
            await store.Initialize();
            store.DropCollection();
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
        
        public static Task<IEventStore> AutoCreateAndInitializeEventStore(
            [CallerFilePath] string sourceFilePath = "",
            [CallerMemberName] string callMemberName = ""
        )
        {
            var sourceFileName = sourceFilePath
                .Split(new[] {"\\", "/"}, StringSplitOptions.None)
                .Last()
                .Replace(".cs", "");

            return CreateAndInitializeEventStore(sourceFileName, callMemberName)
                .Map(store => (IEventStore) store);
        }
    }
}
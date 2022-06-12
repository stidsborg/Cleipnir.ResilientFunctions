using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.StressTests;
using MongoDB.Driver;

namespace Cleipnir.ResilientFunctions.MongoDB.StressTest;

public class Helper : IHelper
{
    private const string ConnectionString = "mongodb://root:Pa55word!@localhost:27017?authSource=admin";

    public async Task InitializeDatabaseAndInitializeAndTruncateTable()
    {
        var store = new MongoDbFunctionStore(ConnectionString, "rfunctions_stresstest");
        await store.DropUnderlyingCollection();
    } 
    public async Task<int> NumberOfNonCompleted()
    {
        var dbClient = new MongoClient(ConnectionString);
        var db = dbClient.GetDatabase("rfunctions_stresstest");
        var collection = db.GetCollection<Document>("rfunctions");
        var postponedStatus = (int) Status.Postponed;
        var executingStatus = (int) Status.Executing;
        
        return (int) await collection.CountDocumentsAsync(d => d.Status == postponedStatus || d.Status == executingStatus);
    }

    public async Task<int> NumberOfSuccessfullyCompleted()
    {
        var dbClient = new MongoClient(ConnectionString);
        var db = dbClient.GetDatabase("rfunctions_stresstest");
        var collection = db.GetCollection<Document>("rfunctions");
        var succeededStatus = (int) Status.Succeeded;

        return (int) await collection.CountDocumentsAsync(d => d.Status == succeededStatus);
    }

    public Task<IFunctionStore> CreateFunctionStore()
    {
        var store = new MongoDbFunctionStore(ConnectionString, "rfunctions_stresstest");
        return store.CastTo<IFunctionStore>().ToTask();
    } 
    
    private record Document
    {
        public DbFunctionId Id { get; set; } = new();
        public string ParameterJson { get; set; } = "";
        public string ParameterType { get; set; } = "";
        public string? ScrapbookJson { get; set; }
        public string? ScrapbookType { get; set; }
        public int Status { get; set; }
        public string? ResultJson { get; set; }
        public string? ResultType { get; set; }
        public string? ErrorJson { get; set; }
        public long? PostponedUntil { get; set; }
        public int Epoch { get; set; }
        public int SignOfLife { get; set; }
    }

    private class DbFunctionId
    {
        public string FunctionTypeId { get; set; } = "";
        public string FunctionInstanceId { get; set; } = "";
    }
}
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using MongoDB.Driver;

namespace Cleipnir.ResilientFunctions.MongoDB;

public class MongoDbEventStore : IEventStore
{
    private readonly string _connectionString;
    private readonly string _databaseName;
    private readonly string _collectionName;
    
    public MongoDbEventStore(string connectionString, string databaseName, string collectionName = "events")
    {
        _connectionString = connectionString;
        _databaseName = databaseName;
        _collectionName = collectionName;
    }

    public Task Initialize()
    {
        var collection = GetCollection();
        var notificationLogBuilder = Builders<Document>.IndexKeys;
        var indexModel = new CreateIndexModel<Document>(notificationLogBuilder
            .Ascending(d => d.Id.FunctionTypeId)
            .Ascending(d => d.Id.FunctionInstanceId)
            .Ascending(d => d.Id.Position)
        );
        
        return collection!.Indexes.CreateOneAsync(indexModel);
    }

    public void DropCollection()
    {
        var dbClient = new MongoClient(_connectionString);
        var db = dbClient.GetDatabase(_databaseName);
        db.DropCollection(_collectionName);
    }
    
    public async Task AppendEvent(FunctionId functionId, StoredEvent storedEvent)
    {
        var functionTypeId = functionId.TypeId.Value;
        var functionInstanceId = functionId.InstanceId.Value;
        
        var collection = GetCollection();

        var count = await collection.CountDocumentsAsync(d =>
            d.Id.FunctionTypeId == functionTypeId && d.Id.FunctionInstanceId == functionInstanceId
        );
        
        var document = new Document
        {
            Id = new DbFunctionId
            {
                FunctionTypeId = functionId.TypeId.Value,
                FunctionInstanceId = functionId.InstanceId.Value,
                Position = (int) count
            },
            EventJson = storedEvent.EventJson,
            EventType = storedEvent.EventType,
            IdempotencyKey = storedEvent.IdempotencyKey
        };
        
        try
        {
            await collection!.InsertOneAsync(document);
        }
        catch (MongoWriteException exception) when(exception.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            await AppendEvent(functionId, storedEvent);
        }
    }

    public Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));

    public async Task AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        var functionTypeId = functionId.TypeId.Value;
        var functionInstanceId = functionId.InstanceId.Value;

        var collection = GetCollection();
        
        var count = await collection.CountDocumentsAsync(d =>
            d.Id.FunctionTypeId == functionTypeId && d.Id.FunctionInstanceId == functionInstanceId
        );

        var documents = storedEvents
            .Select((storedEvent, i) => new Document
            {
                Id = new DbFunctionId
                {
                    FunctionTypeId = functionId.TypeId.Value,
                    FunctionInstanceId = functionId.InstanceId.Value,
                    Position = i + (int) count
                },
                EventJson = storedEvent.EventJson,
                EventType = storedEvent.EventType,
                IdempotencyKey = storedEvent.IdempotencyKey
            });

        await collection!.InsertManyAsync(documents);
    }

    public async Task Truncate(FunctionId functionId)
    {
        var functionTypeId = functionId.TypeId.Value;
        var functionInstanceId = functionId.InstanceId.Value;
        await GetCollection()!.DeleteManyAsync(d => d.Id.FunctionTypeId == functionTypeId && d.Id.FunctionInstanceId == functionInstanceId);
    }

    public Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip)
    {
        var functionTypeId = functionId.TypeId.Value;
        var functionInstanceId = functionId.InstanceId.Value;
        var collection = GetCollection();

        var query =
            from d in collection.AsQueryable()
            where d.Id.FunctionTypeId == functionTypeId &&  
                  d.Id.FunctionInstanceId == functionInstanceId &&
                  d.Id.Position >= skip
                  select new { d.EventJson, d.EventType, d.IdempotencyKey };
        
        var sef = new List<StoredEvent>();
        foreach (var row in query)
            sef.Add(new StoredEvent(row.EventJson, row.EventType, row.IdempotencyKey));

        return sef.CastTo<IEnumerable<StoredEvent>>().ToTask();
    }
    
    private IMongoCollection<Document>? GetCollection()
    {
        var dbClient = new MongoClient(_connectionString); 
        var db = dbClient.GetDatabase(_databaseName);
        var collection = db.GetCollection<Document>(_collectionName);
        return collection;
    }
    
    private record Document
    {
        public DbFunctionId Id { get; set; } = new();
        public string EventJson { get; set; } = "";
        public string EventType { get; set; } = "";
        public string? IdempotencyKey { get; set; }
    }

    private class DbFunctionId
    {
        public string FunctionTypeId { get; set; } = "";
        public string FunctionInstanceId { get; set; } = "";
        public int Position { get; set; }
    }
}
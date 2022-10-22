using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using MongoDB.Driver;

namespace Cleipnir.ResilientFunctions.MongoDB;

public class MongoDbFunctionStore : IFunctionStore
{
    private readonly string _connectionString;
    private readonly string _databaseName;
    private readonly string _collectionName;
    
    public MongoDbFunctionStore(string connectionString, string databaseName, string collectionName = "rfunctions")
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
            .Ascending(d => d.Status)
        );
        
        return collection!.Indexes.CreateOneAsync(indexModel);
    }
    
    public Task DropUnderlyingCollection()
    {
        var dbClient = new MongoClient(_connectionString);
        var db = dbClient.GetDatabase(_databaseName);
        db.DropCollection(_collectionName);

        return Task.CompletedTask;
    }

    private IMongoCollection<Document>? GetCollection()
    {
        var dbClient = new MongoClient(_connectionString); 
        var db = dbClient.GetDatabase(_databaseName);
        var collection = db.GetCollection<Document>(_collectionName);
        return collection;
    }

    public async Task<bool> CreateFunction(FunctionId functionId, StoredParameter param, StoredScrapbook storedScrapbook, long crashedCheckFrequency, int version)
    {
        var document = new Document
        {
            Id = new DbFunctionId
            {
                FunctionTypeId = functionId.TypeId.Value,
                FunctionInstanceId = functionId.InstanceId.Value
            },
            ParameterJson = param.ParamJson,
            ParameterType = param.ParamType,
            ScrapbookJson = storedScrapbook.ScrapbookJson,
            ScrapbookType = storedScrapbook.ScrapbookType,
            Status = (int) Status.Executing,
            Epoch = 0,
            SignOfLife = 0,
            CrashedCheckFrequency = crashedCheckFrequency,
            Version = version
        };

        var collection = GetCollection();
        try
        {
            await collection!.InsertOneAsync(document);
        }
        catch (MongoWriteException exception) when(exception.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            return false;
        }
        
        return true;
    }
    
    public async Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch, long crashedCheckFrequency, int version)
    {
        var functionTypeId = functionId.TypeId.Value;
        var functionInstanceId = functionId.InstanceId.Value;
        var status = (int) newStatus;

        var collection = GetCollection();
        
        var update = Builders<Document>
            .Update
            .Set(d => d.Status, status)
            .Set(d => d.Epoch, newEpoch)
            .Set(d => d.CrashedCheckFrequency, crashedCheckFrequency)
            .Set(d => d.Version, version);

        var result = await collection.UpdateOneAsync(
            d =>
                d.Id.FunctionTypeId == functionTypeId &&
                d.Id.FunctionInstanceId == functionInstanceId &&
                d.Epoch == expectedEpoch,
            update
        );

        var modified = result.ModifiedCount;
        return modified == 1;
    }

    public async Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
    {
        var functionTypeId = functionId.TypeId.Value;
        var functionInstanceId = functionId.InstanceId.Value;

        var collection = GetCollection();

        var update = Builders<Document>
            .Update
            .Set(d => d.SignOfLife, newSignOfLife);

        var result = await collection.UpdateOneAsync(
            d =>
                d.Id.FunctionTypeId == functionTypeId &&
                d.Id.FunctionInstanceId == functionInstanceId &&
                d.Epoch == expectedEpoch,
            update
        );

        var modified = result.ModifiedCount;
        return modified == 1;
    }

    public Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId, int versionUpperBound)
    {
        var functionTypeIdStr = functionTypeId.Value;
        var executingStatus = (int) Status.Executing;
        var collection = GetCollection();

        var query =
            from d in collection.AsQueryable()
            where d.Id.FunctionTypeId == functionTypeIdStr && d.Status == executingStatus && d.Version <= versionUpperBound
            select new { d.Id.FunctionInstanceId, d.Epoch, d.SignOfLife, d.CrashedCheckFrequency };
        
        var sef = new List<StoredExecutingFunction>();
        foreach (var row in query)
            sef.Add(new StoredExecutingFunction(row.FunctionInstanceId, row.Epoch, row.SignOfLife, row.CrashedCheckFrequency));

        return sef.CastTo<IEnumerable<StoredExecutingFunction>>().ToTask();
    }

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore, int versionUpperBound)
    {
        var functionTypeIdStr = functionTypeId.Value;
        var postponedStatus = (int) Status.Postponed;
        var collection = GetCollection();

        var query =
            from d in collection.AsQueryable()
            where d.Id.FunctionTypeId == functionTypeIdStr && d.Status == postponedStatus && 
                  d.PostponedUntil <= expiresBefore && d.Version <= versionUpperBound
            select new { d.Id.FunctionInstanceId, d.Epoch, d.PostponedUntil };
        
        var sef = new List<StoredPostponedFunction>();
        foreach (var row in query)
            sef.Add(new StoredPostponedFunction(row.FunctionInstanceId, row.Epoch, row.PostponedUntil!.Value));

        return sef.CastTo<IEnumerable<StoredPostponedFunction>>().ToTask();

    }

    public async Task<bool> SetFunctionState(FunctionId functionId, Status status, string scrapbookJson, StoredResult? result, string? errorJson, long? postponedUntil, int expectedEpoch)
    {
        var functionTypeId = functionId.TypeId.Value;
        var functionInstanceId = functionId.InstanceId.Value;

        var collection = GetCollection();

        var update = Builders<Document>
            .Update
            .Set(d => d.Status, (int) status)
            .Set(d => d.ScrapbookJson, scrapbookJson)
            .Set(d => d.ErrorJson, errorJson)
            .Set(d => d.ResultJson, result?.ResultJson)
            .Set(d => d.ResultType, result?.ResultType)
            .Set(d => d.PostponedUntil, postponedUntil);

        var updateResult = await collection.UpdateOneAsync(
            d =>
                d.Id.FunctionTypeId == functionTypeId &&
                d.Id.FunctionInstanceId == functionInstanceId &&
                d.Epoch == expectedEpoch,
            update
        );

        var modified = updateResult.ModifiedCount;
        return modified == 1;
    }

    public async Task<bool> SetScrapbook(FunctionId functionId, string scrapbookJson, int expectedEpoch)
    {
        var functionTypeId = functionId.TypeId.Value;
        var functionInstanceId = functionId.InstanceId.Value;

        var collection = GetCollection();

        var update = Builders<Document>
            .Update
            .Set(d => d.ScrapbookJson, scrapbookJson);

        var updateResult = await collection.UpdateOneAsync(
            d =>
                d.Id.FunctionTypeId == functionTypeId &&
                d.Id.FunctionInstanceId == functionInstanceId &&
                d.Epoch == expectedEpoch,
            update
        );

        var modified = updateResult.MatchedCount;
        return modified == 1;
    }

    public async Task<bool> SetParameters(FunctionId functionId, StoredParameter? storedParameter, StoredScrapbook? storedScrapbook, int expectedEpoch)
    {
        var functionTypeId = functionId.TypeId.Value;
        var functionInstanceId = functionId.InstanceId.Value;

        var collection = GetCollection();

        if (storedParameter != null && storedScrapbook != null)
        {
            var update = Builders<Document>
                .Update
                .Set(d => d.ParameterJson, storedParameter.ParamJson)
                .Set(d => d.ParameterType, storedParameter.ParamType)
                .Set(d => d.ScrapbookJson, storedScrapbook.ScrapbookJson)
                .Set(d => d.ScrapbookType, storedScrapbook.ScrapbookType);

            var updateResult = await collection.UpdateOneAsync(
                d =>
                    d.Id.FunctionTypeId == functionTypeId &&
                    d.Id.FunctionInstanceId == functionInstanceId &&
                    d.Epoch == expectedEpoch,
                update
            );

            var modified = updateResult.MatchedCount;
            return modified == 1;            
        }

        if (storedParameter == null && storedScrapbook != null)
        {
            var update = Builders<Document>
                .Update
                .Set(d => d.ScrapbookJson, storedScrapbook.ScrapbookJson)
                .Set(d => d.ScrapbookType, storedScrapbook.ScrapbookType);            

            var updateResult = await collection.UpdateOneAsync(
                d =>
                    d.Id.FunctionTypeId == functionTypeId &&
                    d.Id.FunctionInstanceId == functionInstanceId &&
                    d.Epoch == expectedEpoch,
                update
            );

            var modified = updateResult.MatchedCount;
            return modified == 1;
        }
        
        if (storedParameter != null && storedScrapbook == null)
        {
            var update = Builders<Document>
                .Update
                .Set(d => d.ParameterJson, storedParameter.ParamJson)
                .Set(d => d.ParameterType, storedParameter.ParamType);

            var updateResult = await collection.UpdateOneAsync(
                d =>
                    d.Id.FunctionTypeId == functionTypeId &&
                    d.Id.FunctionInstanceId == functionInstanceId &&
                    d.Epoch == expectedEpoch,
                update
            );

            var modified = updateResult.MatchedCount;
            return modified == 1;
        }

        return true;
    }

    public async Task<bool> FailFunction(FunctionId functionId, string errorJson, string scrapbookJson, int expectedEpoch)
    {
        var functionTypeId = functionId.TypeId.Value;
        var functionInstanceId = functionId.InstanceId.Value;

        var collection = GetCollection();

        var update = Builders<Document>
            .Update
            .Set(d => d.Status, (int)Status.Failed)
            .Set(d => d.ErrorJson, errorJson)
            .Set(d => d.ScrapbookJson, scrapbookJson);

        var updateResult = await collection.UpdateOneAsync(
            d =>
                d.Id.FunctionTypeId == functionTypeId &&
                d.Id.FunctionInstanceId == functionInstanceId &&
                d.Epoch == expectedEpoch,
            update
        );

        var modified = updateResult.MatchedCount;
        return modified == 1;
    }

    public async Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        var functionTypeId = functionId.TypeId.Value;
        var functionInstanceId = functionId.InstanceId.Value;

        var document = await GetCollection()
            .Find(d =>
                d.Id.FunctionTypeId == functionTypeId &&
                d.Id.FunctionInstanceId == functionInstanceId
            ).SingleOrDefaultAsync();
        
        if (document == null) return null;

        return new StoredFunction(
            new FunctionId(document.Id.FunctionTypeId, document.Id.FunctionInstanceId),
            new StoredParameter(document.ParameterJson, document.ParameterType),
            new StoredScrapbook(document.ScrapbookJson, document.ScrapbookType),
            (Status) document.Status,
            document.ResultType == null ? null : new StoredResult(document.ResultJson, document.ResultType),
            document.ErrorJson,
            document.PostponedUntil,
            document.Version,
            document.Epoch,
            document.SignOfLife,
            document.CrashedCheckFrequency
        );
    }

    public async Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null, Status? expectedStatus = null)
    {
        var functionTypeId = functionId.TypeId.Value;
        var functionInstanceId = functionId.InstanceId.Value;

        if (expectedEpoch == null && expectedStatus == null)
        {
            var result = await GetCollection().DeleteOneAsync(d =>
                d.Id.FunctionTypeId == functionTypeId &&
                d.Id.FunctionInstanceId == functionInstanceId
            );
            return result.DeletedCount == 1;
        }
        
        if (expectedEpoch != null && expectedStatus == null)
        {
            var epoch = expectedEpoch.Value;
            var result = await GetCollection().DeleteOneAsync(d =>
                d.Id.FunctionTypeId == functionTypeId &&
                d.Id.FunctionInstanceId == functionInstanceId &&
                d.Epoch == epoch
            );
            return result.DeletedCount == 1;
        }
        
        if (expectedEpoch == null && expectedStatus != null)
        {
            var status = (int) expectedStatus.Value;
            var result = await GetCollection().DeleteOneAsync(d =>
                d.Id.FunctionTypeId == functionTypeId &&
                d.Id.FunctionInstanceId == functionInstanceId &&
                d.Status == status
            );
            return result.DeletedCount == 1;
        }
        
        //then it must be that (expectedEpoch != null && expectedStatus != null)
        {
            var epoch = expectedEpoch!.Value;
            var status = (int) expectedStatus!.Value;
            var result = await GetCollection().DeleteOneAsync(d =>
                d.Id.FunctionTypeId == functionTypeId &&
                d.Id.FunctionInstanceId == functionInstanceId &&
                d.Epoch == epoch &&
                d.Status == status
            );
            return result.DeletedCount == 1;
        }
    }

    private record Document
    {
        public DbFunctionId Id { get; set; } = new();
        public string ParameterJson { get; set; } = "";
        public string ParameterType { get; set; } = "";
        public string ScrapbookJson { get; set; } = "";
        public string ScrapbookType { get; set; } = "";
        public int Status { get; set; }
        public string? ResultJson { get; set; }
        public string? ResultType { get; set; }
        public string? ErrorJson { get; set; }
        public long? PostponedUntil { get; set; }
        public int Epoch { get; set; }
        public int SignOfLife { get; set; }
        public long CrashedCheckFrequency { get; set; }
        public int Version { get; set; }
    }

    private class DbFunctionId
    {
        public string FunctionTypeId { get; set; } = "";
        public string FunctionInstanceId { get; set; } = "";
    }
}
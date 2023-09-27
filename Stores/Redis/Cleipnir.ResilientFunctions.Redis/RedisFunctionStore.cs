using System.Runtime.Serialization;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using StackExchange.Redis;

namespace Cleipnir.ResilientFunctions.Redis;

public class RedisFunctionStore : IFunctionStore
{
    public IEventStore EventStore { get; } 
    public ITimeoutStore TimeoutStore { get; }
    public Utilities Utilities { get; }
    
    private readonly ConnectionMultiplexer _redis;

    public RedisFunctionStore(string connectionString)
    {
        _redis = ConnectionMultiplexer.Connect(connectionString);
        EventStore = new RedisEventStore(_redis);
    }
    
    public Task Initialize()
    {
        _ = _redis.GetDatabase();
        return Task.CompletedTask;
    }

    public async Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        StoredScrapbook storedScrapbook,
        IEnumerable<StoredEvent>? storedEvents, 
        long leaseExpiration, 
        long? postponeUntil)
    {
        if (storedEvents != null)
            await EventStore.AppendEvents(functionId, storedEvents);

        leaseExpiration = RoundToMilliSecondPrecision(leaseExpiration); //due to Redis representing scores as floating point numbers
        postponeUntil = postponeUntil == null ? null : RoundToMilliSecondPrecision(postponeUntil.Value);
        
        const string script = @"
            local success = redis.call('HSETNX', KEYS[1], 'Epoch', ARGV[1])
            if success == 0 then return false end
            
            redis.call('HSET', KEYS[1], 'Status', ARGV[2], 'ParamType', ARGV[3], 'ParamJson', ARGV[4], 'ScrapbookType', ARGV[5], 'ScrapbookJson', ARGV[6], 'PostponeUntil', ARGV[7], 'LeaseExpiration', ARGV[8])            

            if (ARGV[2] == '3') 
            then 
                redis.call('ZADD', KEYS[3], ARGV[11], ARGV[12])
            else 
                redis.call('ZADD', KEYS[2], ARGV[9], ARGV[10])
            end
            return true
        ";

        const int initialEpoch = 0;
        var result = await _redis.GetDatabase().ScriptEvaluateAsync(
            script,
            keys: new RedisKey[]
            {
                GetStateKeyName(functionId),
                GetExecutingKeyName(functionId),
                GetPostponedKeyName(functionId)
            },
            values: new RedisValue[]
            {
                initialEpoch, 
                (int) (postponeUntil == null ? Status.Executing : Status.Postponed),
                param.ParamType,
                param.ParamJson,
                storedScrapbook.ScrapbookType,
                storedScrapbook.ScrapbookJson,
                postponeUntil ?? -1L,
                leaseExpiration,
                leaseExpiration,
                $"0,{leaseExpiration},{functionId.InstanceId}",
                postponeUntil ?? -1L,
                $"0,{postponeUntil},{functionId.InstanceId}",
            }
        );
        
        return (bool)result;
    }

    public async Task<bool> IncrementAlreadyPostponedFunctionEpoch(FunctionId functionId, int expectedEpoch)
    {
        const string script = @"
            local epoch = redis.call('HGET', KEYS[1], 'Epoch')
            if epoch == nil then return false end
            if epoch ~= ARGV[1] then return false end
            
            redis.call('HSET', KEYS[1], 'Epoch', ARGV[2]) 
            redis.call('ZREM', KEYS[2], ARGV[5])                                       
            redis.call('ZADD', KEYS[2], ARGV[6], ARGV[7])
            return true
        ";

        var newEpoch = expectedEpoch + 1;
        
        var result = await _redis.GetDatabase().ScriptEvaluateAsync(
            script,
            keys: new RedisKey[]
            {
                GetStateKeyName(functionId),
                GetPostponedKeyName(functionId)
            },
            values: new RedisValue[]
            {
                expectedEpoch,
                newEpoch,
                $"{expectedEpoch},{functionId.InstanceId}",
                DateTime.UtcNow.Ticks,
                $"{newEpoch},{functionId.InstanceId}"
            }
        );

        return (bool)result;
    }

    public async Task<bool> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        const string script = @"
            local epoch = redis.call('HGET', KEYS[1], 'Epoch')
            if epoch == nil then return false end
            if epoch ~= ARGV[1] then return false end
            
            redis.call('HSET', KEYS[1], 'Status', ARGV[2], 'LeaseExpiration', ARGV[3], 'Epoch', ARGV[4]) 
            redis.call('ZREM', KEYS[2], ARGV[5])
            redis.call('ZREM', KEYS[3], ARGV[5])                                       
            redis.call('ZADD', KEYS[2], ARGV[6], ARGV[7])
            return true
        ";

        leaseExpiration = RoundToMilliSecondPrecision(leaseExpiration);
        var newEpoch = expectedEpoch + 1;
        
        var result = await _redis.GetDatabase().ScriptEvaluateAsync(
            script,
            keys: new RedisKey[]
            {
                GetStateKeyName(functionId),
                GetExecutingKeyName(functionId),
                GetPostponedKeyName(functionId)
            },
            values: new RedisValue[]
            {
                expectedEpoch,
                (int) Status.Executing,
                leaseExpiration,
                newEpoch,
                $"{expectedEpoch},{functionId.InstanceId}",
                leaseExpiration,
                $"{newEpoch},{functionId.InstanceId}"
            }
        );

        return (bool)result;
    }

    public async Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        const string script = @"
            local epoch = redis.call('HGET', KEYS[1], 'Epoch')
            if epoch == nil then return false end
            if epoch ~= ARGV[1] then return false end
                                                          
            redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
            return true
        ";
        
        var result = await _redis.GetDatabase().ScriptEvaluateAsync(
            script,
            keys: new RedisKey[]
            {
                GetStateKeyName(functionId),
                GetExecutingKeyName(functionId),
            },
            values: new RedisValue[]
            {
                expectedEpoch,
                leaseExpiration,
                $"{expectedEpoch},{functionId.InstanceId}",
            }
        );

        return (bool)result;
    }

    public async Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore)
    {
        var expiredFunctions = await _redis.GetDatabase().SortedSetRangeByScoreWithScoresAsync(
            GetExecutingKeyName(functionTypeId),
            stop: leaseExpiresBefore
        );

        var epochAndInstanceIdPairs = expiredFunctions
            .Select(sse =>
                {
                    var splitValue = sse.Element.ToString().Split(',', count: 2);
                    var score = (long)sse.Score;
                    return new StoredExecutingFunction(
                        InstanceId: splitValue[2],
                        Epoch: int.Parse(splitValue[0]),
                        LeaseExpiration: score * 1000
                    );
                }
            )
            .Where(sef => sef.LeaseExpiration <= leaseExpiresBefore);

        return epochAndInstanceIdPairs;
    }

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
    {
        throw new NotImplementedException();
    }

    public Task<bool> SetFunctionState(
        FunctionId functionId, Status status, StoredParameter storedParameter,
        StoredScrapbook storedScrapbook, StoredResult storedResult, StoredException? storedException, 
        long? postponeUntil,
        int expectedEpoch)
    {
        
        throw new NotImplementedException();
    }

    public async Task<bool> SaveScrapbookForExecutingFunction(FunctionId functionId, string scrapbookJson, int expectedEpoch, ComplimentaryState.SaveScrapbookForExecutingFunction complimentaryState)
    {
        const string script = @"
            local epoch = redis.call('HGET', KEYS[1], 'Epoch')
            if epoch == nil then return false end
            if epoch ~= ARGV[1] then return false end
            
            redis.call('HSET', KEYS[1], 'ScrapbookJson', ARGV[2]) 
            return true
        ";

        var success = await _redis.GetDatabase().ScriptEvaluateAsync(
            script,
            keys: new RedisKey[]
            {
                GetStateKeyName(functionId),
                GetExecutingKeyName(functionId)
            },
            values: new RedisValue[]
            {
                expectedEpoch,
                scrapbookJson
            }
        );

        return (bool) success;
    }

    public Task<bool> SetParameters(
        FunctionId functionId, 
        StoredParameter storedParameter, 
        StoredScrapbook storedScrapbook, 
        int expectedEpoch)
    {
        throw new NotImplementedException();
    }

    public async Task<bool> SucceedFunction(
        FunctionId functionId, 
        StoredResult result, 
        string scrapbookJson, 
        int expectedEpoch,
        ComplimentaryState.SetResult complementaryState)
    {
        const string script = @"
            local epoch = redis.call('HGET', KEYS[1], 'Epoch')
            if epoch == nil then return false end
            if epoch ~= ARGV[1] then return false end
            
            redis.call('HSET', KEYS[1], 'Status', ARGV[2], 'ResultIsNull', ARGV[3], 'ResultJson', ARGV[4], 'ResultType', ARGV[5], 'ScrapbookJson', ARGV[6]) 
            redis.call('ZREM', KEYS[2], ARGV[7])
            return true
        ";
        
        var success = await _redis.GetDatabase().ScriptEvaluateAsync(
            script,
            keys: new RedisKey[]
            {
                GetStateKeyName(functionId),
                GetExecutingKeyName(functionId)
            },
            values: new RedisValue[]
            {
                expectedEpoch, 
                (int) Status.Succeeded,
                result.ResultType == null,
                result.ResultJson ?? "",
                result.ResultType ?? "",
                scrapbookJson,
                $"{expectedEpoch},{functionId.InstanceId}"
            }
        );

        return (bool) success;
    }

    public async Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult complementaryState)
    {
        const string script = @"
            local epoch = redis.call('HGET', KEYS[1], 'Epoch')
            if epoch == nil then return false end
            if epoch ~= ARGV[1] then return false end
            
            redis.call('HSET', KEYS[1], 'Status', ARGV[2], 'PostponeUntil', ARGV[3], 'ScrapbookJson', ARGV[4]) 
            redis.call('ZREM', KEYS[2], ARGV[5])
            redis.call('ZADD', KEYS[3], ARGV[6], ARGV[7])
            return true
        ";
        
        var result = await _redis.GetDatabase().ScriptEvaluateAsync(
            script,
            keys: new RedisKey[]
            {
                GetStateKeyName(functionId),
                GetExecutingKeyName(functionId),
                GetPostponedKeyName(functionId)
            },
            values: new RedisValue[]
            {
                expectedEpoch, 
                (int) Status.Postponed,
                postponeUntil,
                scrapbookJson,
                $"{expectedEpoch},{functionId.InstanceId}",
                postponeUntil,
                $"{expectedEpoch},{functionId.InstanceId}"
            }
        );

        return (bool) result;
    }

    public async Task<bool> FailFunction(
        FunctionId functionId, 
        StoredException storedException, 
        string scrapbookJson, 
        int expectedEpoch,
        ComplimentaryState.SetResult complementaryState)
    {
        const string script = @"
            local epoch = redis.call('HGET', KEYS[1], 'Epoch')
            if epoch == nil then return false end
            if epoch ~= ARGV[1] then return false end
            
            redis.call('HSET', KEYS[1], 'Status', ARGV[2], 'StoredException', ARGV[3], 'ScrapbookJson', ARGV[4]) 
            redis.call('ZREM', KEYS[2], ARGV[5])
            return true
        ";
        
        var result = await _redis.GetDatabase().ScriptEvaluateAsync(
            script,
            keys: new RedisKey[]
            {
                GetStateKeyName(functionId),
                GetExecutingKeyName(functionId)
            },
            values: new RedisValue[]
            {
                expectedEpoch, 
                (int) Status.Failed,
                SerializeStoredException(storedException),
                scrapbookJson,
                $"{expectedEpoch},{functionId.InstanceId}"
            }
        );

        return (bool) result;
    }

    public Task<SuspensionResult> SuspendFunction(
        FunctionId functionId, 
        int expectedEventCount, string scrapbookJson, int expectedEpoch,
        ComplimentaryState.SetResult complementaryState)
    {
        throw new NotImplementedException();
    }

    public async Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
    {
        //todo handle non-existing entity by returning null
        var values = await _redis.GetDatabase().HashGetAsync(
            GetStateKeyName(functionId),
            hashFields: new RedisValue[] { "Status", "Epoch" }
        );

        if (values.Any(v => v.IsNull)) return null;
        
        return new StatusAndEpoch((Status)(int)values[0], (int)values[1]);
    }

    public async Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        var allKeysAndValues = await _redis.GetDatabase().HashGetAllAsync(GetStateKeyName(functionId));

        if (allKeysAndValues.Length == 0)
            return null;

        int? epoch = null;
        Status? status = null;
        string? paramType = null;
        string? paramJson = null;
        string? scrapbookType = null;
        string? scrapbookJson = null;
        bool resultIsNull = true;
        string? resultType = null;
        string? resultJson = null;
        string? storedException = null;
        long? postponedUntil = null;
        long? leaseExpiration = null;
        
        foreach (var keysAndValues in allKeysAndValues)
        {
            var key = keysAndValues.Name.ToString();
            var value = keysAndValues.Value;
            if (key == "Epoch")
                epoch = (int)value;
            else if (key == "Status")
                status = (Status)(int)value;
            else if (key == "ParamType")
                paramType = (string)value!;
            else if (key == "ParamJson")
                paramJson = (string)value!;
            else if (key == "ScrapbookType")
                scrapbookType = (string)value!;
            else if (key == "ScrapbookJson")
                scrapbookJson = (string)value!;
            else if (key == "ResultIsNull")
                resultIsNull = (bool) value!;
            else if (key == "ResultType")
                resultType = (string)value!;
            else if (key == "ResultJson")
                resultJson = (string)value!;
            else if (key == "StoredException")
                storedException = (string)value!;
            else if (key == "PostponeUntil")
                postponedUntil = value == -1 ? null : (long)value;
            else if (key == "LeaseExpiration")
                leaseExpiration = (long)value!;
        }

        if (epoch == null || status == null || paramType == null || paramJson == null || scrapbookType == null || scrapbookJson == null || leaseExpiration == null)
            throw new SerializationException($"Unable to deserialize state for function: '{functionId}'");

        return new StoredFunction(
            functionId,
            new StoredParameter(paramJson, paramType),
            new StoredScrapbook(scrapbookJson, scrapbookType),
            status.Value,
            resultIsNull ? new StoredResult(null, null) : new StoredResult(resultJson, resultType),
            storedException == null ? null : DeserializeStoredException(functionId, storedException),
            postponedUntil,
            epoch.Value,
            leaseExpiration.Value
        );
    }

    public async Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
    {
        var db = _redis.GetDatabase();
        var sf = await GetFunction(functionId);
        if (sf == null || (expectedEpoch != null && expectedEpoch.Value != sf.Epoch))
            return false;

        db.SortedSetRemove(GetExecutingKeyName(functionId), member: $"{expectedEpoch},{functionId.InstanceId}");
        db.SortedSetRemove(GetPostponedKeyName(functionId), member: $"{expectedEpoch},{functionId.InstanceId}");

        db.KeyDelete(GetStateKeyName(functionId));
        return true;
    }

    internal static string GetStateKeyName(FunctionId functionId)
    {
        var (functionTypeId, functionInstanceId) = functionId;
        var typeIdBase64 = Base64.Encode(functionTypeId.ToString());
        var instanceIdBase64 = Base64.Encode(functionInstanceId.ToString());
        return $"{typeIdBase64}_{instanceIdBase64}_state";
    }

    private static string GetExecutingKeyName(FunctionTypeId functionTypeId)
    {
        var typeIdBase64 = Base64.Encode(functionTypeId.ToString());
        return $"{typeIdBase64}_executing";
    }

    private static string GetExecutingKeyName(FunctionId functionId) => GetExecutingKeyName(functionId.TypeId);
    
    private static string GetPostponedKeyName(FunctionId functionId)
    {
        var (functionTypeId, _) = functionId;
        var typeIdBase64 = Base64.Encode(functionTypeId.ToString());
        return $"{typeIdBase64}_postponed";
    }

    private static string SerializeStoredException(StoredException storedException)
    {
        var (exceptionMessage, exceptionStackTrace, exceptionType) = storedException;
        return SimpleDictionaryMarshaller.Serialize(
            new Dictionary<string, string?>
            {
                { $"{nameof(StoredException.ExceptionMessage)}", exceptionMessage },
                { $"{nameof(StoredException.ExceptionStackTrace)}", exceptionStackTrace },
                { $"{nameof(StoredException.ExceptionType)}", exceptionType },
            }
        );
    }

    private static StoredException DeserializeStoredException(FunctionId functionId, string storedException)
    {
        var dict = SimpleDictionaryMarshaller.Deserialize(storedException, expectedCount: 3);

        return new StoredException(
            dict[nameof(StoredException.ExceptionMessage)] ?? throw new SerializationException($"Unable to deserialize exception state for '{functionId}'"),
            dict[nameof(StoredException.ExceptionStackTrace)],
            dict[nameof(StoredException.ExceptionType)] ?? throw new SerializationException($"Unable to deserialize exception state for '{functionId}'")
        );
    }


    private static long RoundToMilliSecondPrecision(long ticks)
    {
        const long nsPerSecond = 10000000;
        return ticks / (nsPerSecond * 1000);
    }
}
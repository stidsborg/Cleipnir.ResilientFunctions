using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage.Utils;
using StackExchange.Redis;

namespace Cleipnir.ResilientFunctions.Redis;

public class RedisEventStore : IEventStore
{
    private readonly ConnectionMultiplexer _redis;

    public RedisEventStore(string connectionString) : this(ConnectionMultiplexer.Connect(connectionString)) {} 
    public RedisEventStore(ConnectionMultiplexer connectionMultiplexer) => _redis = connectionMultiplexer;
    
    public Task Initialize()
    {
        _ = _redis.GetDatabase();
        return Task.CompletedTask;
    }

    public async Task<FunctionStatus> AppendEvent(FunctionId functionId, StoredEvent storedEvent)
    {
        var serializedEvent = SimpleMarshaller.Serialize(storedEvent.EventJson, storedEvent.EventType, storedEvent.IdempotencyKey);
        
        const string script = @"            
            redis.call('RPUSH', KEYS[1], ARGV[1])            
            local epoch = redis.call('HGET', KEYS[2], 'Epoch')
            local status = redis.call('HGET', KEYS[2], 'Status')
            local arr = {}
            arr[1] = epoch
            arr[2] = status

            return arr             
        ";
        
        var result = await _redis.GetDatabase().ScriptEvaluateAsync(
            script,
            keys: new RedisKey[]
            {
                GetEventsKeyName(functionId),
                RedisFunctionStore.GetStateKeyName(functionId)
            },
            values: new RedisValue[]
            {
                serializedEvent
            }
        );
        var arr = (int[])result!;
        return new FunctionStatus((Status)arr[1], Epoch: arr[0]);
    }

    public Task<FunctionStatus> AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null) 
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));

    public async Task<FunctionStatus> AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        var db = _redis.GetDatabase();
        await db.ListRightPushAsync(
            GetEventsKeyName(functionId),
            values: storedEvents
                .Select(se => SimpleMarshaller.Serialize(se.EventJson, se.EventType, se.IdempotencyKey))
                .Select(s => new RedisValue(s))
                .ToArray()
        );

        var values = await db.HashGetAsync(
            RedisFunctionStore.GetStateKeyName(functionId),
            hashFields: new RedisValue[] { "Epoch", "Status" }
        );

        return new FunctionStatus((Status)(int)values[1], (int)values[0]);
    }

    public async Task Truncate(FunctionId functionId) 
        => await _redis.GetDatabase().KeyDeleteAsync(GetEventsKeyName(functionId));

    public async Task Replace(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        var db = _redis.GetDatabase();
        var transaction = db.CreateTransaction();
        var key = GetEventsKeyName(functionId);
        _ = transaction.KeyDeleteAsync(key);

        _ = transaction.ListRightPushAsync(key, values: 
            storedEvents
                .Select(se => SimpleMarshaller.Serialize(se.EventJson, se.EventType, se.IdempotencyKey))
                .Select(s => new RedisValue(s))
                .ToArray()
        );

        await transaction.ExecuteAsync();
    }

    public async Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId)
    {
        var idempotencyKeys = new HashSet<string>();
        var events = await _redis.GetDatabase().ListRangeAsync(GetEventsKeyName(functionId));
        var storedEvents = events.Select(rv => (string)rv!)
            .Select(s => SimpleMarshaller.Deserialize(s, expectedCount: 3))
            .Select(l => new StoredEvent(l[0]!, l[1]!, l[2]))
            .Where(se =>
            {
                if (se.IdempotencyKey == null) return true;
                if (idempotencyKeys.Contains(se.IdempotencyKey)) return false;
                idempotencyKeys.Add(se.IdempotencyKey);
                return true;
            })
            .ToList();

        return storedEvents;
    }

    public EventsSubscription SubscribeToEvents(FunctionId functionId)
    {
        var atEvent = 0;
        var db = _redis.GetDatabase();
        var keyName = GetEventsKeyName(functionId);
        var idempotencyKeys = new HashSet<string>();
        
        var es = new EventsSubscription(
            pullEvents: async () =>
            {
                var events = await db.ListRangeAsync(
                    keyName,
                    atEvent
                );
                if (!events.Any())
                    return ArraySegment<StoredEvent>.Empty;

                atEvent += events.Length;
                
                var storedEvents = events.Select(rv => (string)rv!)
                    .Select(s => SimpleMarshaller.Deserialize(s, expectedCount: 3))
                    .Select(l => new StoredEvent(EventJson: l[0]!, EventType: l[1]!, IdempotencyKey: l[2]))
                    .Where(se =>
                    {
                        if (se.IdempotencyKey == null) return true;
                        if (idempotencyKeys.Contains(se.IdempotencyKey)) return false;
                        idempotencyKeys.Add(se.IdempotencyKey);
                        return true;
                    })    
                    .ToList();
                
                return storedEvents;
            }, dispose: () => ValueTask.CompletedTask
        );

        return es;
    }
    
    private static string GetEventsKeyName(FunctionId functionId)
    {
        var (functionTypeId, functionInstanceId) = functionId;
        var typeIdBase64 = Base64.Encode(functionTypeId.ToString());
        var instanceIdBase64 = Base64.Encode(functionInstanceId.ToString());
        return $"{typeIdBase64}_{instanceIdBase64}_events";
    }
}
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Queuing;

public delegate bool MessagePredicate(object message);

public class QueueManager(StoredId storedId, IMessageStore messageStore, ISerializer serializer)
{
    private readonly Dictionary<int, Subscription> _subscribers = new();
    private readonly Lock _lock = new();
    private int _nextId;
    
    public async Task Run()
    {
        var toDeliver = new SortedDictionary<int, MessageWithPosition>();
        var max = 0;
        while (true)
        {
            var messages = await messageStore.GetMessages(storedId, skip: 0);
            foreach (var (messageContent, messageType, position, idempotencyKey) in messages)
            {
                var msg = serializer.DeserializeMessage(messageContent, messageType);
                toDeliver[max++] = new MessageWithPosition(msg, position, idempotencyKey);
            }

            foreach (var (key, messageWithPosition) in toDeliver.ToList())
            {
                var (message, position, idempotencyKey) = messageWithPosition;
                foreach (var kv in _subscribers.ToList())
                {
                    var subscriberId = kv.Key;
                    var subscriber = kv.Value;
                    if (subscriber.Predicate(message))
                    {
                        _subscribers.Remove(subscriberId);
                        subscriber.Tcs.SetResult(messageWithPosition);
                        toDeliver.Remove(key);
                    }
                }
            }
        }
    }
    
    public Task<MessageWithPosition> Subscribe(MessagePredicate predicate)
    {
        var tcs = new TaskCompletionSource<MessageWithPosition>();
        lock (_lock)
        {
            var id = _nextId++;
            _subscribers[id] = new Subscription(predicate, tcs);
        }
        
        return tcs.Task;
    }

    public record MessageWithPosition(object Message, long Position, string? IdempotencyKey);
    private record Subscription(MessagePredicate Predicate, TaskCompletionSource<MessageWithPosition> Tcs);
}
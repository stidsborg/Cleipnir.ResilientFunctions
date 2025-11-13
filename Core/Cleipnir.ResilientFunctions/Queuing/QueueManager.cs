using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Queuing;

public delegate bool MessagePredicate(object message);
public delegate Task MessageHandler(object message, long position, string? idempotencyKey);

public class QueueManager(StoredId storedId, IMessageStore messageStore, ISerializer serializer)
{
    private Dictionary<int, Subscriber> _subscribers = new();
    
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

            foreach (var (key, (message, position, idempotencyKey)) in toDeliver.ToList())
            {
                foreach (var subscriber in _subscribers.Values)
                {
                    if (subscriber.Predicate(message))
                    {
                        await subscriber.Handler(messages, position, idempotencyKey);
                        toDeliver.Remove(key);
                    }
                }
            }
        }
    }

    private record MessageWithPosition(object Message, long Position, string? IdempotencyKey);

    private record Subscriber(int Id, MessagePredicate Predicate, MessageHandler Handler);
}
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class Postman(StoredType storedType, ICorrelationStore correlationStore, MessageWriters messageWriters)
{
    public Task SendMessage<TMessage>(
        StoredId instance, 
        TMessage message, 
        string? idempotencyKey = null
    ) where TMessage : notnull => messageWriters.For(instance).AppendMessage(message, idempotencyKey);
    
    public async Task SendMessages(IReadOnlyList<BatchedMessage> messages)
        => await messageWriters.AppendMessages(messages);

    public async Task RouteMessage<TMessage>(TMessage message, string correlationId, string? idempotencyKey = null) where TMessage : notnull
    {
        var flowInstances = await correlationStore.GetCorrelations(storedType, correlationId);
        foreach (var storedId in flowInstances)
            await SendMessage(storedId, message, idempotencyKey);
    }
}
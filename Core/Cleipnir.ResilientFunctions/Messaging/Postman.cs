using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class Postman(StoredType storedType, ICorrelationStore correlationStore, MessageWriters messageWriters)
{
    public Task<Finding> SendMessage<TMessage>(
        StoredInstance instance, 
        TMessage message, 
        string? idempotencyKey = null
    ) where TMessage : notnull => messageWriters.For(instance).AppendMessage(message, idempotencyKey);

    public async Task RouteMessage<TMessage>(TMessage message, string correlationId, string? idempotencyKey = null) where TMessage : notnull
    {
        var flowInstances = await correlationStore.GetCorrelations(storedType, correlationId);
        foreach (var flowInstance in flowInstances)
            await SendMessage(flowInstance, message, idempotencyKey);
    }
}
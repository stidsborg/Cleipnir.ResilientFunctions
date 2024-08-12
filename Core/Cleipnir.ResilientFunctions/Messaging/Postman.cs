using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class Postman
{
    private readonly ICorrelationStore _correlationStore;
    private readonly MessageWriters _messageWriters;
    private readonly FlowType _flowType;

    public Postman(
        FlowType flowType,
        ICorrelationStore correlationStore, 
        MessageWriters messageWriters)
    {
        _flowType = flowType;
        _correlationStore = correlationStore;
        _messageWriters = messageWriters;
    }
    
    public Task<Finding> SendMessage<TMessage>(
        FlowInstance instance, 
        TMessage message, 
        string? idempotencyKey = null
    ) where TMessage : notnull => _messageWriters.For(instance).AppendMessage(message, idempotencyKey);

    public async Task RouteMessage<TMessage>(TMessage message, string correlationId, string? idempotencyKey = null) where TMessage : notnull
    {
        var flowInstances = await _correlationStore.GetCorrelations(_flowType, correlationId);
        foreach (var flowInstance in flowInstances)
            await SendMessage(flowInstance, message, idempotencyKey);
    }
}
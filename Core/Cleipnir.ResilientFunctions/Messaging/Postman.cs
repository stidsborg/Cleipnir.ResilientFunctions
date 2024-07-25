using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class Postman
{
    private readonly FrozenDictionary<Type, RouteResolver> _routes;
    private readonly ICorrelationStore _correlationStore;
    private readonly MessageWriters _messageWriters;
    private readonly Func<FlowInstance, Task> _scheduleParamless;
    private readonly FlowType _flowType;

    public Postman(
        FlowType flowType,
        IEnumerable<RoutingInformation> routes, 
        ICorrelationStore correlationStore, 
        MessageWriters messageWriters,
        Func<FlowInstance, Task> scheduleParamless)
    {
        _flowType = flowType;
        _routes = routes.ToFrozenDictionary(
            i => i.MessageType,
            i => i.RouteResolver
        );
        _correlationStore = correlationStore;
        _messageWriters = messageWriters;
        _scheduleParamless = scheduleParamless;
    }

    public async Task RouteMessage<TMessage>(TMessage message) where TMessage : notnull
    {
        var messageType = typeof(TMessage);
        var success = _routes.TryGetValue(messageType, out var routeResolver);
        if (!success) return;

        var routingInfo = routeResolver!(message);
        await RouteMessage(message, routingInfo);
    }

    public async Task RouteMessage<TMessage>(TMessage message, RoutingInfo routingInfo) where TMessage : notnull
    {
        if (routingInfo.CorrelationId is not null)
        {
            var flowInstances = await _correlationStore.GetCorrelations(_flowType, routingInfo.CorrelationId);
            
            foreach (var flowInstance in flowInstances)
            {
                var messageWriter = _messageWriters.For(flowInstance);
                var finding = await messageWriter.AppendMessage(message, routingInfo.IdempotencyKey);
                if (finding == Finding.NotFound)
                    await _scheduleParamless(flowInstance);
            }
        }
        else
        {
            var messageWriter = _messageWriters.For(routingInfo.FlowInstanceId!);
            var finding = await messageWriter.AppendMessage(message, routingInfo.IdempotencyKey);   
                
            if (finding == Finding.NotFound)
                await _scheduleParamless(routingInfo.FlowInstanceId!);
        }
    }
}
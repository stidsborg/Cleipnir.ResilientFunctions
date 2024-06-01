using System;

namespace Cleipnir.ResilientFunctions.Domain;

public record RoutingInfo(
    string? FlowInstanceId,
    string? CorrelationId,
    string? IdempotencyKey
);

public record RoutingInformation(
    Type MessageType,
    RouteResolver RouteResolver 
);

public record RoutingInformation<TMessage>(RouteResolver<TMessage> Resolver)  
    : RoutingInformation(MessageType: typeof(TMessage), msg => Resolver((TMessage) msg)) where TMessage : notnull
{
    public RouteResolver<TMessage> Resolver { get; init; } = Resolver;
}

public delegate RoutingInfo RouteResolver<TMessage>(TMessage message) where TMessage : notnull;
public delegate RoutingInfo RouteResolver(object message);

public static class Route
{
    public static RoutingInfo To(string flowInstance, string? idempotencyKey = null)
    {
        return new RoutingInfo(flowInstance, CorrelationId: null, idempotencyKey);
    }

    public static RoutingInfo Using(string correlationId, string? idempotencyKey = null)
    {
        return new RoutingInfo(FlowInstanceId: null, correlationId, idempotencyKey);
    }
}
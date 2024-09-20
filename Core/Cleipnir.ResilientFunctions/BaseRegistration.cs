using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public delegate Task<IReadOnlyList<FlowInstance>> GetInstances(Status? status = null);

public abstract class BaseRegistration
{
    protected Postman Postman { get; } 
    
    public GetInstances GetInstances { get; }

    protected BaseRegistration(Postman postman, GetInstances getInstances)
    {
        Postman = postman;
        GetInstances = getInstances;
    } 

    public Task RouteMessage<T>(T message, string correlationId, string? idempotencyKey = null) where T : notnull 
        => Postman.RouteMessage(message, correlationId, idempotencyKey);
}
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public delegate Task<IReadOnlyList<StoredInstance>> GetInstances(Status? status = null);

public abstract class BaseRegistration
{
    protected Postman Postman { get; } 
    
    public GetInstances GetInstances { get; }
    public StoredType StoredType { get; }

    protected BaseRegistration(StoredType storedType, Postman postman, GetInstances getInstances)
    {
        StoredType = storedType;
        Postman = postman;
        GetInstances = getInstances;
    } 

    public Task RouteMessage<T>(T message, string correlationId, string? idempotencyKey = null) where T : notnull 
        => Postman.RouteMessage(message, correlationId, idempotencyKey);

    public StoredId MapToStoredId(FlowId id) => new(StoredType, id.Instance.Value.ToStoredInstance());
}
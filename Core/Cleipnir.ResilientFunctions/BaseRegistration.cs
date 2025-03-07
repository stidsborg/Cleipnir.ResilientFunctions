using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public abstract class BaseRegistration
{
    private readonly IFunctionStore _functionStore;
    protected Postman Postman { get; } 
    public StoredType StoredType { get; }

    protected BaseRegistration(StoredType storedType, Postman postman, IFunctionStore functionStore)
    {
        _functionStore = functionStore;
        StoredType = storedType;
        Postman = postman;
    } 

    public Task RouteMessage<T>(T message, string correlationId, string? idempotencyKey = null) where T : notnull 
        => Postman.RouteMessage(message, correlationId, idempotencyKey);
    
    public StoredId MapToStoredId(FlowInstance instance) => new(StoredType, instance.Value.ToStoredInstance());

    public async Task<IReadOnlyList<StoredInstance>> GetInstances(Status? status = null)
    {
        return await (status == null
                ? _functionStore.GetInstances(StoredType)
                : _functionStore.GetInstances(StoredType, status.Value)
            );
    }

    public Task Interrupt(IEnumerable<FlowInstance> instances)
        => Interrupt(instances.Select(i => i.ToStoredInstance()));
    public async Task Interrupt(IEnumerable<StoredInstance> storedInstances) 
        => await _functionStore.Interrupt(storedInstances.Select(si => new StoredId(StoredType, si)));
}
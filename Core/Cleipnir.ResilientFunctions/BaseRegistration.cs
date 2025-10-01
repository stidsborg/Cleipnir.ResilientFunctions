using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public abstract class BaseRegistration
{
    private readonly IFunctionStore _functionStore;
    protected Postman Postman { get; } 
    public StoredType StoredType { get; }
    protected UtcNow UtcNow { get; }

    protected BaseRegistration(StoredType storedType, Postman postman, IFunctionStore functionStore, UtcNow utcNow)
    {
        _functionStore = functionStore;
        StoredType = storedType;
        Postman = postman;
        UtcNow = utcNow;
    } 

    public Task RouteMessage<T>(T message, string correlationId, string? idempotencyKey = null) where T : notnull 
        => Postman.RouteMessage(message, correlationId, idempotencyKey);
    
    public StoredId MapToStoredId(FlowInstance instance) => new(instance.Value.ToStoredInstance(StoredType));

    public async Task<IReadOnlyList<StoredId>> GetInstances(Status? status = null)
    {
        return await (status == null
                ? _functionStore.GetInstances(StoredType)
                : _functionStore.GetInstances(StoredType, status.Value)
            );
    }

    public Task Interrupt(IEnumerable<FlowInstance> instances)
        => Interrupt(instances.Select(i => i.ToStoredInstance(StoredType).ToStoredId()));
    public async Task Interrupt(IEnumerable<StoredId> storedIds) 
        => await _functionStore.Interrupt(storedIds.ToList());
}
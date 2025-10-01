using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IFunctionStore
{
    public ITypeStore TypeStore { get; }
    public IMessageStore MessageStore { get; }
    public IEffectsStore EffectsStore { get; }
    public ICorrelationStore CorrelationStore { get; }
    public Utilities Utilities { get; }
    public ISemaphoreStore SemaphoreStore { get; }
    public IReplicaStore ReplicaStore { get; }
    public Task Initialize();
    
    Task<bool> CreateFunction(
        StoredId storedId, 
        FlowInstance humanInstanceId,
        byte[]? param,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp,
        StoredId? parent,
        ReplicaId? owner,
        IReadOnlyList<StoredEffect>? effects = null,
        IReadOnlyList<StoredMessage>? messages = null
    );
    
    Task BulkScheduleFunctions(
        IEnumerable<IdWithParam> functionsWithParam,
        StoredId? parent
    );
    
    Task<StoredFlowWithEffectsAndMessages?> RestartExecution(StoredId storedId, ReplicaId owner);
    
    Task<IReadOnlyList<StoredId>> GetExpiredFunctions(long expiresBefore);
    Task<IReadOnlyList<StoredId>> GetSucceededFunctions(StoredType storedType, long completedBefore);
    
    Task<bool> SetParameters(
        StoredId storedId,
        byte[]? param,
        byte[]? result,
        ReplicaId? expectedReplica
    );
    
    Task<bool> SetFunctionState(
        StoredId storedId,
        Status status,
        byte[]? param,
        byte[]? result,
        StoredException? storedException,
        long expires,
        ReplicaId? expectedReplica
    );

    Task<bool> SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState
    );
    
    Task<bool> PostponeFunction(
        StoredId storedId,
        long postponeUntil, 
        long timestamp,
        bool ignoreInterrupted, 
        ReplicaId expectedReplica, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState
    );
    
    Task<bool> FailFunction(
        StoredId storedId, 
        StoredException storedException,
        long timestamp,
        ReplicaId expectedReplica, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState
    );
    
    Task<bool> SuspendFunction(
        StoredId storedId, 
        long timestamp,
        ReplicaId expectedReplica, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState
    );

    Task<IReadOnlyList<ReplicaId>> GetOwnerReplicas();
    Task RescheduleCrashedFunctions(ReplicaId replicaId);

    Task<bool> Interrupt(StoredId storedId);
    Task Interrupt(IReadOnlyList<StoredId> storedIds);
    Task<bool?> Interrupted(StoredId storedId); 

    Task<Status?> GetFunctionStatus(StoredId storedId);
    Task<IReadOnlyList<StatusAndId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds);
    Task<StoredFlow?> GetFunction(StoredId storedId);
    Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType, Status status);
    Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType);

    Task<bool> DeleteFunction(StoredId storedId);

    IFunctionStore WithPrefix(string prefix);
}
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage.Session;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IFunctionStore
{
    public ITypeStore TypeStore { get; }
    public IMessageStore MessageStore { get; }
    public IEffectsStore EffectsStore { get; }
    public IReplicaStore ReplicaStore { get; }
    public Task Initialize();
    
    Task<IStorageSession?> CreateFunction(
        StoredId storedId,
        FlowInstance humanInstanceId,
        byte[]? param,
        long? postponeUntil,
        long timestamp,
        StoredId? parent,
        ReplicaId? owner,
        IReadOnlyList<StoredEffect>? effects = null
    );
    
    Task<int> BulkScheduleFunctions(
        IEnumerable<IdWithParam> functionsWithParam,
        StoredId? parent
    );
    
    Task<Dictionary<StoredId, StoredFlowWithEffects>> RestartExecutions(IReadOnlyList<StoredId> storedIds, ReplicaId owner);
    
    Task<IReadOnlyList<StoredId>> GetExpiredFunctions(long expiresBefore);
    Task<IReadOnlyList<StoredId>> GetSucceededFunctions(long completedBefore);
    
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

    /// <summary>
    /// Transitions a function out of the Executing state into a terminal-or-parked status
    /// (Succeeded, Failed, Postponed or Suspended), releasing ownership. Consolidates the former
    /// SucceedFunction/FailFunction/PostponeFunction/SuspendFunction methods.
    /// <para>
    /// <paramref name="result"/> is only relevant for <see cref="Status.Succeeded"/>,
    /// <paramref name="storedException"/> only for <see cref="Status.Failed"/>, and
    /// <paramref name="expires"/> is the postpone-until timestamp for <see cref="Status.Postponed"/>
    /// (0 for the other statuses).
    /// </para>
    /// </summary>
    Task<bool> SetStatus(
        StoredId storedId,
        Status status,
        byte[]? result,
        StoredException? storedException,
        long expires,
        long timestamp,
        ReplicaId expectedReplica,
        IStorageSession? storageSession
    );

    Task<IReadOnlyList<ReplicaId>> GetOwnerReplicas();
    Task RescheduleCrashedFunctions(ReplicaId replicaId);

    Task<Status?> GetFunctionStatus(StoredId storedId);
    Task<IReadOnlyList<StatusAndId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds);
    Task<StoredFlow?> GetFunction(StoredId storedId);

    Task<bool> DeleteFunction(StoredId storedId);

    IFunctionStore WithPrefix(string prefix);
    Task<IReadOnlyDictionary<StoredId, byte[]?>> GetResults(IEnumerable<StoredId> storedIds);
}
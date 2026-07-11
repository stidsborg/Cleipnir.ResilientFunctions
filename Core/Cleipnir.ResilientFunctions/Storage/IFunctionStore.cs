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
        IReadOnlyList<StoredEffect>? effects = null,
        IReadOnlyList<StoredMessage>? messages = null
    );
    
    Task<int> BulkScheduleFunctions(
        IEnumerable<IdWithParam> functionsWithParam,
        StoredId? parent
    );
    
    Task<StoredFlowWithEffects?> ClaimFunction(StoredId storedId, ReplicaId owner);
    Task<Dictionary<StoredId, StoredFlowWithEffects>> ClaimFunctions(IReadOnlyList<StoredId> storedIds, ReplicaId owner);
    
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
    /// Owner-guarded general state-setter used by a replica that currently owns a flow to atomically move it to a
    /// target <paramref name="status"/>, write an updated effect snapshot and set/release ownership. The write only
    /// lands while the flow's owner column still equals <paramref name="expectedReplica"/>; on a guard mismatch
    /// nothing is written and false is returned. The caller must already own the flow (claim it first), which is
    /// why <paramref name="expectedReplica"/> is required.
    /// <para>
    /// <paramref name="owner"/> is the new owner value written to the row - null releases the flow (owner → NULL).
    /// <paramref name="effects"/> null leaves the effects column untouched; non-null overwrites the effect snapshot
    /// with the supplied list. <paramref name="param"/>, <paramref name="result"/> and <paramref name="exception"/>
    /// are always written to the supplied value (null included).
    /// </para>
    /// <para>
    /// <paramref name="storageSession"/> is the store's opaque forward-carried per-flow context (typically the one
    /// handed back by <see cref="ClaimFunction"/>). When supplied together with a non-null <paramref name="effects"/>
    /// snapshot, it is kept coherent with the just-persisted snapshot so it can be reused for later effect writes;
    /// null disables session threading.
    /// </para>
    /// </summary>
    /// <returns>True iff the guard matched and the row was updated; otherwise false.</returns>
    Task<bool> SetFunction(
        StoredId storedId,
        Status status,
        byte[]? param,
        byte[]? result,
        StoredException? exception,
        long expires,
        long timestamp,
        ReplicaId? owner,
        IReadOnlyList<StoredEffect>? effects,
        ReplicaId expectedReplica,
        IStorageSession? storageSession
    );

    Task<bool> SucceedFunction(
        StoredId storedId,
        byte[]? result,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession
    );
    
    Task<bool> PostponeFunction(
        StoredId storedId,
        long postponeUntil,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession
    );
    
    Task<bool> FailFunction(
        StoredId storedId,
        StoredException storedException,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession
    );
    
    Task<bool> SuspendFunction(
        StoredId storedId,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
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
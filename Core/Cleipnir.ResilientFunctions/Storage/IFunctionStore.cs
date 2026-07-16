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

    // Effects live in the 'effects' column on the flows row. Reads go through GetFunction (StoredFlow.Effects);
    // there is deliberately no separate effect-read method. Writes come in three modes:
    // - owned session (SnapshotStorageSession with ReplicaId): the session's snapshot is serialized and written
    //   guarded by the owner column alone - the claim protocol serializes these writes.
    // - unowned session (SnapshotStorageSession with null ReplicaId): the snapshot write is guarded by
    //   owner IS NULL and the flow's effect version (see SnapshotStorageSession.Version).
    // - null session: the store itself atomically reads owner/version/effects, applies the changes and writes
    //   guarded by both read values - used when no loaded snapshot exists (e.g. control-panel writes).
    // A failed guard throws UnexpectedStateException.ConcurrentModification in every mode.
    Task SetEffectResult(StoredId storedId, StoredEffectChange storedEffectChange, IStorageSession? session)
        => SetEffectResults(storedId, changes: [storedEffectChange], session);
    Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session);
    Task DeleteEffectResult(StoredId storedId, EffectId effectId, IStorageSession? storageSession)
        => SetEffectResults(
            storedId,
            changes: [new StoredEffectChange(storedId, effectId, CrudOperation.Delete, StoredEffect: null)],
            storageSession
        );
}
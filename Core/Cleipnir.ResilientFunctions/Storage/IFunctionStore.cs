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
    /// <para>
    /// Implementations must NOT persist <paramref name="storageSession"/>'s effect snapshot as part of the
    /// status write: effect persistence flows exclusively through the serialized flush
    /// (<see cref="SetEffectResults"/>), which the runtime performs before every status transition. Writing
    /// the session snapshot here would re-write potentially stale effect state outside the flush
    /// serialization, clobbering concurrently flushed writes.
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
    // there is deliberately no separate effect-read method. The owner argument is the owner the write is
    // conditioned on: the write only succeeds while the flow's owner column still has this value. Null demands
    // that the flow is unowned (owner IS NULL) - the guard used when writing to a completed flow's effects, so
    // a concurrent claim makes the write fail instead of being silently overwritten by the claimant's later
    // flushes. Writes come in three modes:
    // - owned write (session with non-null owner): the session's snapshot is serialized and written
    //   guarded by the owner column alone - the claim protocol serializes these writes.
    // - unowned write (session with null owner): the snapshot write is guarded by
    //   owner IS NULL and the flow's effect version (see SnapshotStorageSession.Version).
    // - null session: the store itself atomically reads owner/version/effects, applies the changes and writes
    //   guarded by both read values - used when no loaded snapshot exists (e.g. control-panel writes); the
    //   owner argument is ignored.
    // A failed guard throws UnexpectedStateException.ConcurrentModification in every mode.
    Task SetEffectResult(StoredId storedId, StoredEffectChange storedEffectChange, ReplicaId? owner, IStorageSession? session)
        => SetEffectResults(storedId, changes: [storedEffectChange], owner, session);
    Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, ReplicaId? owner, IStorageSession? session);
    Task DeleteEffectResult(StoredId storedId, EffectId effectId, ReplicaId? owner, IStorageSession? storageSession)
        => SetEffectResults(
            storedId,
            changes: [new StoredEffectChange(storedId, effectId, CrudOperation.Delete, StoredEffect: null)],
            owner,
            storageSession
        );
}
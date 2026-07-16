using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.Storage.Session;

public class SnapshotStorageSession(ReplicaId? replicaId) : IStorageSession
{
    /// <summary>
    /// The owner the effect write is conditioned on: the write only succeeds while the flow's owner column still
    /// has this value. Null demands that the flow is unowned (owner IS NULL) - the guard used when writing to a
    /// completed flow's effects, so a concurrent claim makes the write fail instead of being silently overwritten
    /// by the claimant's later flushes.
    /// </summary>
    public ReplicaId? ReplicaId { get; private init; } = replicaId;

    public Dictionary<EffectId, StoredEffect> Effects { get; } = new();

    /// <summary>
    /// The flow's effect version at the time <see cref="Effects"/> was loaded. Only honored when
    /// <see cref="ReplicaId"/> is null: unowned writes serialize the whole effects column from the in-memory
    /// snapshot, so the store additionally guards on the version (bumped by every unowned write and every claim)
    /// to prove nothing changed since the load - otherwise the write fails with a concurrent-modification error.
    /// Owned writes are already serialized by the owner guard and ignore the version.
    /// </summary>
    public int Version { get; set; }

    public byte[] Serialize() => Serialize(Effects);

    public static byte[] Serialize(Dictionary<EffectId, StoredEffect> effects)
    {
        var parts = effects
            .Values
            .Select(e => e.Serialize())
            .ToArray();

        return BinaryPacker.Pack(parts);
    }
}

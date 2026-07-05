using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.Storage.Session;

public class SnapshotStorageSession(ReplicaId? replicaId) : IStorageSession
{
    /// <summary>
    /// Sentinel for <see cref="Version"/> disabling the version comparison in stores that use it - the write is
    /// then guarded solely by the owner comparison (used by writers without a version snapshot, e.g. the
    /// pending-message inlining of completed flows).
    /// </summary>
    public const int NoVersionCheck = -1;

    /// <summary>
    /// The owner the effect write is conditioned on: the write only succeeds while the flow's owner column still
    /// has this value. Null demands that the flow is unowned (owner IS NULL) - the guard used when writing to a
    /// completed flow's effects, so a concurrent claim makes the write fail instead of being silently overwritten
    /// by the claimant's later flushes.
    /// </summary>
    public ReplicaId? ReplicaId { get; private init; } = replicaId;

    public Dictionary<EffectId, StoredEffect> Effects { get; } = new();
    public int Version { get; set; }
    public bool RowExists { get; set; }

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

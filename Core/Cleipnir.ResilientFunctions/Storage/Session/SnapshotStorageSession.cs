using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.Storage.Session;

public class SnapshotStorageSession : IStorageSession
{
    public Dictionary<EffectId, StoredEffect> Effects { get; } = new();

    /// <summary>
    /// The flow's effect version at the time <see cref="Effects"/> was loaded. Only honored when the owner
    /// passed to the store's effect write is null: unowned writes serialize the whole effects column from the
    /// in-memory snapshot, so the store additionally guards on the version (bumped by every unowned write and
    /// every claim) to prove nothing changed since the load - otherwise the write fails with a
    /// concurrent-modification error. Owned writes are already serialized by the owner guard and ignore the
    /// version.
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

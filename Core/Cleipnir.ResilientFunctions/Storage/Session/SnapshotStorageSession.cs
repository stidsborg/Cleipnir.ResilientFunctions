using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.Storage.Session;

public class SnapshotStorageSession(ReplicaId replicaId) : IStorageSession
{
    public ReplicaId ReplicaId { get; private init; } = replicaId;

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
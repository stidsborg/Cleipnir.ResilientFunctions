using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.Storage.Session;

public class SnapshotStorageSession : IStorageSession
{
    public Dictionary<EffectId, StoredEffect>  Effects { get; } = new();

    public byte[] Serialize()
    {
        var parts = Effects
            .Values
            .Select(e => e.Serialize())
            .ToArray();
        
        return BinaryPacker.Pack(parts);
    }
}
using System;
using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage.Session;

public class PositionsStorageSession : IStorageSession
{
    public Dictionary<SerializedEffectId, long> Positions { get; } = new();
    public long MaxPosition { get; set; } = -1;

    public long Add(SerializedEffectId id)
    {
        var position = ++MaxPosition;
        Positions[id] = position;
        return position;
    }

    public void Set(SerializedEffectId id, long position)
    {
        MaxPosition = Math.Max(MaxPosition, position);
        Positions[id] = position;
    }

    public void Remove(SerializedEffectId id) => Positions.Remove(id);

    public long? Get(SerializedEffectId id)  => Positions.TryGetValue(id, out var position) ? position : null;
}
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
}
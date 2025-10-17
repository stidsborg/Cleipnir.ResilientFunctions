using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage.Session;

public class PositionsStorageSession : IStorageSession
{
    public Dictionary<string, long> Positions { get; } = new();
    public long MaxPosition { get; set; }

    public void Add(EffectId effectId)
    {
        var serializedEffectId = effectId.Serialize();
        if (Positions.ContainsKey(serializedEffectId))
            return;
        
        var maxPosition = MaxPosition;
        MaxPosition++;
        Positions[serializedEffectId] = maxPosition;
    }
}
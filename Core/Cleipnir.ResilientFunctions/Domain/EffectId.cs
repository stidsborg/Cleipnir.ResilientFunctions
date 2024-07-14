using System;

namespace Cleipnir.ResilientFunctions.Domain;

public class EffectId
{
    public string Value { get; }
    public EffectId(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Delimiters.EnsureNoUnitSeparator(value);
        
        Value = value;
    }
    
    public static implicit operator EffectId(string flowInstance) => new(flowInstance);
    public override string ToString() => Value;
    public static bool operator ==(EffectId id1, EffectId id2) => id1.Equals(id2);
    public static bool operator !=(EffectId id1, EffectId id2) => !(id1 == id2);

    public override bool Equals(object? obj)
        => obj is EffectId id && id.Value == Value;
    public override int GetHashCode() => Value.GetHashCode();
}
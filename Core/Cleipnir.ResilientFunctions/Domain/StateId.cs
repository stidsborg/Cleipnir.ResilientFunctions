using System;

namespace Cleipnir.ResilientFunctions.Domain;

public class StateId
{
    public string Value { get; }
    public StateId(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Delimiters.EnsureNoUnitSeparator(value);
        
        Value = value;
    }
    
    public static implicit operator StateId(string functionInstanceId) => new(functionInstanceId);
    public override string ToString() => Value;
    public static bool operator ==(StateId id1, StateId id2) => id1.Equals(id2);
    public static bool operator !=(StateId id1, StateId id2) => !(id1 == id2);

    public override bool Equals(object? obj)
        => obj is EffectId id && id.Value == Value;
    public override int GetHashCode() => Value.GetHashCode();
}
using System;

namespace Cleipnir.ResilientFunctions.Domain;

public class FlowInstance
{
    public string Value { get; }
    public FlowInstance(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Value = value;
    }
    
    public static implicit operator FlowInstance(string flowInstance) => new(flowInstance);
    public override string ToString() => Value;
    public static bool operator ==(FlowInstance id1, FlowInstance id2) => id1.Equals(id2);
    public static bool operator !=(FlowInstance id1, FlowInstance id2) => !(id1 == id2);

    public override bool Equals(object? obj)
        => obj is FlowInstance id && id.Value == Value;
    public override int GetHashCode() => Value.GetHashCode();
}
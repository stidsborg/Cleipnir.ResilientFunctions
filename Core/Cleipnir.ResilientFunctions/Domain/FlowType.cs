using System;

namespace Cleipnir.ResilientFunctions.Domain;

public class FlowType
{
    public string Value { get; }
    public FlowType(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Value = value;
    }
    
    public static implicit operator FlowType(string flowType) => new(flowType);
    public override string ToString() => Value;
    public static bool operator ==(FlowType id1, FlowType id2) => id1.Equals(id2);
    public static bool operator !=(FlowType id1, FlowType id2) => !(id1 == id2);
    
    public override bool Equals(object? obj)
        => obj is FlowType id && id.Value == Value;
    public override int GetHashCode() => Value.GetHashCode();
}
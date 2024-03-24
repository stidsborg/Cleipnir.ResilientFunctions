using System;

namespace Cleipnir.ResilientFunctions.Domain;

public class FunctionInstanceId
{
    public string Value { get; }
    public FunctionInstanceId(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Delimiters.EnsureNoUnitSeparator(value);
        
        Value = value;
    }
    
    public static implicit operator FunctionInstanceId(string functionInstanceId) => new(functionInstanceId);
    public override string ToString() => Value;
    public static bool operator ==(FunctionInstanceId id1, FunctionInstanceId id2) => id1.Equals(id2);
    public static bool operator !=(FunctionInstanceId id1, FunctionInstanceId id2) => !(id1 == id2);

    public override bool Equals(object? obj)
        => obj is FunctionInstanceId id && id.Value == Value;
    public override int GetHashCode() => Value.GetHashCode();
}
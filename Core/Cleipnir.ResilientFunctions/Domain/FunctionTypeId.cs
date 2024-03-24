using System;

namespace Cleipnir.ResilientFunctions.Domain;

public class FunctionTypeId
{
    public string Value { get; }
    public FunctionTypeId(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Delimiters.EnsureNoUnitSeparator(value);
        
        Value = value;
    }
    
    public static implicit operator FunctionTypeId(string functionTypeId) => new(functionTypeId);
    public override string ToString() => Value;
    public static bool operator ==(FunctionTypeId id1, FunctionTypeId id2) => id1.Equals(id2);
    public static bool operator !=(FunctionTypeId id1, FunctionTypeId id2) => !(id1 == id2);
    
    public override bool Equals(object? obj)
        => obj is FunctionTypeId id && id.Value == Value;
    public override int GetHashCode() => Value.GetHashCode();
}
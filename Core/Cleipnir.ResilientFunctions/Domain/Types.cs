using System;

namespace Cleipnir.ResilientFunctions.Domain;

public class FunctionTypeId
{
    public string Value { get; }
    public FunctionTypeId(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
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

public class FunctionInstanceId
{
    public string Value { get; }
    public FunctionInstanceId(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
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

public record FunctionId(FunctionTypeId TypeId, FunctionInstanceId InstanceId)
{
    public FunctionId(string functionTypeId, string functionInstanceId) 
        : this(functionTypeId.ToFunctionTypeId(), functionInstanceId.ToFunctionInstanceId()) { }
        
    public FunctionId(FunctionTypeId functionTypeId, string functionInstanceId) 
        : this(functionTypeId, functionInstanceId.ToFunctionInstanceId()) { }

    public FunctionId(string functionTypeId, FunctionInstanceId functionInstanceId) 
        : this(functionTypeId.ToFunctionTypeId(), functionInstanceId) { }

    public override string ToString() 
        => $"{InstanceId}@{TypeId}";
}
    
public static class DomainExtensions
{
    public static FunctionTypeId ToFunctionTypeId(this string functionTypeId)
        => new FunctionTypeId(functionTypeId);

    public static FunctionInstanceId ToFunctionInstanceId(this string functionInstanceId)
        => new FunctionInstanceId(functionInstanceId);
}
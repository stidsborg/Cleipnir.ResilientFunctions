using System;
using System.Text.Json.Serialization;

namespace Cleipnir.ResilientFunctions.Domain;

public class FunctionId
{
    public FunctionTypeId TypeId { get; }
    public FunctionInstanceId InstanceId { get; }
    
    [JsonConstructor]
    public FunctionId(FunctionTypeId typeId, FunctionInstanceId instanceId)
    {
        TypeId = typeId;
        InstanceId = instanceId;
    }
    
    public FunctionId(string functionTypeId, string functionInstanceId) 
        : this(functionTypeId.ToFunctionTypeId(), functionInstanceId.ToFunctionInstanceId()) { }
        
    public FunctionId(FunctionTypeId functionTypeId, string functionInstanceId) 
        : this(functionTypeId, functionInstanceId.ToFunctionInstanceId()) { }

    public FunctionId(string functionTypeId, FunctionInstanceId functionInstanceId) 
        : this(functionTypeId.ToFunctionTypeId(), functionInstanceId) { }

    public override string ToString() 
        => $"{InstanceId}@{TypeId}";

    public override bool Equals(object? obj)
    {
        if (obj == null) return false;
        if (obj is not FunctionId functionId) return false;
        return functionId.TypeId == TypeId && functionId.InstanceId == InstanceId;
    }

    public override int GetHashCode() => HashCode.Combine(TypeId, InstanceId);

    public void Deconstruct(out FunctionTypeId typeId, out FunctionInstanceId instanceId)
    {
        typeId = TypeId;
        instanceId = InstanceId;
    }

    public static bool operator ==(FunctionId? id1, FunctionId? id2)
    {
        if (ReferenceEquals(id1, null) && ReferenceEquals(id2, null))
            return true;
        if (ReferenceEquals(id1, null) || ReferenceEquals(id2, null))
            return false;
        
        return id1.Equals(id2);
    } 
        
    public static bool operator !=(FunctionId? id1, FunctionId? id2) => !(id1 == id2);
}

public static class FunctionIdExtensions 
{
    public static FunctionTypeId ToFunctionTypeId(this string functionTypeId) => new(functionTypeId);

    public static FunctionInstanceId ToFunctionInstanceId(this string functionInstanceId) => new(functionInstanceId);
}
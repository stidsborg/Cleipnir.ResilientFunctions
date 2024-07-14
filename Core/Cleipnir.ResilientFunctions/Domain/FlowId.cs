using System;
using System.Text.Json.Serialization;

namespace Cleipnir.ResilientFunctions.Domain;

public class FlowId
{
    public FlowType Type { get; }
    public FlowInstance Instance { get; }
    
    [JsonConstructor]
    public FlowId(FlowType type, FlowInstance instance)
    {
        Type = type;
        Instance = instance;
    }
    
    public FlowId(string flowType, string flowInstance) 
        : this(flowType.ToFlowType(), flowInstance.ToFlowInstance()) { }
        
    public FlowId(FlowType flowType, string flowInstance) 
        : this(flowType, flowInstance.ToFlowInstance()) { }

    public FlowId(string flowType, FlowInstance flowInstance) 
        : this(flowType.ToFlowType(), flowInstance) { }

    public override string ToString() 
        => $"{Instance}@{Type}";

    public override bool Equals(object? obj)
    {
        if (obj == null) return false;
        if (obj is not FlowId functionId) return false;
        return functionId.Type == Type && functionId.Instance == Instance;
    }

    public override int GetHashCode() => HashCode.Combine(Type, Instance);

    public void Deconstruct(out FlowType type, out FlowInstance instance)
    {
        type = Type;
        instance = Instance;
    }

    public static bool operator ==(FlowId? id1, FlowId? id2)
    {
        if (ReferenceEquals(id1, null) && ReferenceEquals(id2, null))
            return true;
        if (ReferenceEquals(id1, null) || ReferenceEquals(id2, null))
            return false;
        
        return id1.Equals(id2);
    } 
        
    public static bool operator !=(FlowId? id1, FlowId? id2) => !(id1 == id2);

    public FlowId WithInstanceId(FlowInstance instance) => new(Type, instance);
    public FlowId WithTypeId(FlowType type) => new(type, Instance);
}

public static class FlowIdExtensions 
{
    public static FlowType ToFlowType(this string flowType) => new(flowType);

    public static FlowInstance ToFlowInstance(this string flowInstance) => new(flowInstance);
}
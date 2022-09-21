namespace Cleipnir.ResilientFunctions.Domain;

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

public static class FunctionIdExtensions 
{
    public static FunctionTypeId ToFunctionTypeId(this string functionTypeId) => new(functionTypeId);

    public static FunctionInstanceId ToFunctionInstanceId(this string functionInstanceId) => new(functionInstanceId);
}
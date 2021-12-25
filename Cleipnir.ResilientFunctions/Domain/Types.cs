namespace Cleipnir.ResilientFunctions.Domain
{
    public record FunctionTypeId(string Value);
    public record FunctionInstanceId(string Value);

    public record FunctionId(FunctionTypeId TypeId, FunctionInstanceId InstanceId)
    {
        public FunctionId(string functionTypeId, string functionInstanceId) 
            : this(functionTypeId.ToFunctionTypeId(), functionInstanceId.ToFunctionInstanceId()) { }
        
        public FunctionId(FunctionTypeId functionTypeId, string functionInstanceId) 
            : this(functionTypeId, functionInstanceId.ToFunctionInstanceId()) { }

        public FunctionId(string functionTypeId, FunctionInstanceId functionInstanceId) 
            : this(functionTypeId.ToFunctionTypeId(), functionInstanceId) { }
        
        public override string ToString() 
            => $"{nameof(FunctionId)} {{ TypeId = { TypeId.Value }, InstanceId = { InstanceId.Value } }}";
    }
    
    public static class DomainExtensions
    {
        public static FunctionTypeId ToFunctionTypeId(this string functionTypeId)
            => new FunctionTypeId(functionTypeId);

        public static FunctionInstanceId ToFunctionInstanceId(this string functionInstanceId)
            => new FunctionInstanceId(functionInstanceId);
    }
}
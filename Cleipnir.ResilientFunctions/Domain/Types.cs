namespace Cleipnir.ResilientFunctions.Domain
{
    public record FunctionTypeId(string Value);
    public record FunctionInstanceId(string Value);

    public record FunctionId(FunctionTypeId TypeId, FunctionInstanceId InstanceId)
    {
        public FunctionId(string functionTypeId, string functionInstanceId) 
            : this(functionTypeId.ToFunctionTypeId(), functionInstanceId.ToFunctionInstanceId()) { }
    }
    
    public static class DomainExtensions
    {
        public static FunctionTypeId ToFunctionTypeId(this string functionTypeId)
            => new FunctionTypeId(functionTypeId);

        public static FunctionInstanceId ToFunctionInstanceId(this string functionInstanceId)
            => new FunctionInstanceId(functionInstanceId);
    }
}
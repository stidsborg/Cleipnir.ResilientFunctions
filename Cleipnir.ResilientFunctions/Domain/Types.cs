using System.Diagnostics;

namespace Cleipnir.ResilientFunctions.Domain
{
    public record FunctionTypeId(string Value)
    {
        public static implicit operator FunctionTypeId(string functionTypeId) => new(functionTypeId);
    }

    public record FunctionInstanceId(string Value)
    {
        [DebuggerStepThrough]
        public static implicit operator FunctionInstanceId(string functionInstanceId) => new(functionInstanceId);
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
            => $"{ TypeId.Value } { InstanceId.Value }";
    }
    
    public static class DomainExtensions
    {
        public static FunctionTypeId ToFunctionTypeId(this string functionTypeId)
            => new FunctionTypeId(functionTypeId);

        public static FunctionInstanceId ToFunctionInstanceId(this string functionInstanceId)
            => new FunctionInstanceId(functionInstanceId);
    }
}
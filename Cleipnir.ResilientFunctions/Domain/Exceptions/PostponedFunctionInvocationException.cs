namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class PostponedFunctionInvocationException : RFunctionException
{
    public FunctionId FunctionId { get; }

    public PostponedFunctionInvocationException(FunctionId functionId, string message) 
        : base(functionId.TypeId, message) => FunctionId = functionId;
}
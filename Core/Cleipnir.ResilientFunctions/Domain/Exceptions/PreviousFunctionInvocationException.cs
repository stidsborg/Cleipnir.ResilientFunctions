namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class PreviousFunctionInvocationException : RFunctionException
{
    public FunctionId FunctionId { get; }
    public Error Error { get; }

    public PreviousFunctionInvocationException(FunctionId functionId, Error error) 
        : base(functionId.TypeId, $"'{functionId}' function invocation previously failed")
    {
        FunctionId = functionId;
        Error = error;
    }
}
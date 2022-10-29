namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class PreviousFunctionInvocationException : RFunctionException
{
    public FunctionId FunctionId { get; }
    public PreviouslyThrownException Exception { get; }

    public PreviousFunctionInvocationException(FunctionId functionId, PreviouslyThrownException exception) 
        : base(functionId.TypeId, $"'{functionId}' function invocation previously failed")
    {
        FunctionId = functionId;
        Exception = exception;
    }
}
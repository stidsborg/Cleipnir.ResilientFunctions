namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class PreviousFunctionInvocationException : RFunctionException
{
    public FunctionId FunctionId { get; }
    public RError Error { get; }

    public PreviousFunctionInvocationException(FunctionId functionId, RError error, string message) 
        : base(functionId.TypeId, message)
    {
        FunctionId = functionId;
        Error = error;
    }
}
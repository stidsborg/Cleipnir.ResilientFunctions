namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public class EffectException : RFunctionException
{
    public FunctionId FunctionId { get; }
    public PreviouslyThrownException Exception { get; }

    public EffectException(FunctionId functionId, string effectId, PreviouslyThrownException exception) 
        : base(functionId.TypeId, $"Effect '{effectId}' execution for function '{functionId}' failed")
    {
        FunctionId = functionId;
        Exception = exception;
    }
}
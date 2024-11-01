namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public class EffectException(FlowType flowType, string effectId, PreviouslyThrownException exception)
    : FlowTypeException(flowType, $"Effect '{effectId}' execution for '{flowType}' failed")
{
    public PreviouslyThrownException Exception { get; } = exception;
}
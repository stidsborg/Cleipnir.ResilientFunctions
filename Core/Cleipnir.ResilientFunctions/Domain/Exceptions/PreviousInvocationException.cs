namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class PreviousInvocationException(FlowType flowType, PreviouslyThrownException exception)
    : FlowTypeException(flowType, "Previous invocation failed")
{
    public PreviouslyThrownException Exception { get; } = exception;
}
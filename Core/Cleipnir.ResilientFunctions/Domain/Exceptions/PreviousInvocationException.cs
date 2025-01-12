namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class PreviousInvocationException(FlowType flowType, PreviouslyThrownException exception)
    : FlowTypeException(flowType, "Previous invocation failed: " + System.Environment.NewLine + exception.ErrorMessage)
{
    public PreviouslyThrownException Exception { get; } = exception;
}
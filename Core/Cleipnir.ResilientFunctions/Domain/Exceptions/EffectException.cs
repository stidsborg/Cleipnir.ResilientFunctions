namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public class EffectException : FlowTypeException
{
    public FlowId FlowId { get; }
    public PreviouslyThrownException Exception { get; }

    public EffectException(FlowId flowId, string effectId, PreviouslyThrownException exception) 
        : base(flowId.Type, $"Effect '{effectId}' execution for '{flowId}' failed")
    {
        FlowId = flowId;
        Exception = exception;
    }
}
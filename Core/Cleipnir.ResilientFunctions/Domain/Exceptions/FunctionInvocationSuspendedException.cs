namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class FunctionInvocationSuspendedException : RFunctionException
{
    public FlowId FlowId { get; }

    public FunctionInvocationSuspendedException(FlowId flowId)
        : base(
            flowId.Type,
            $"Function '{flowId}' invocation has been suspended"
        ) => FlowId = flowId;
}
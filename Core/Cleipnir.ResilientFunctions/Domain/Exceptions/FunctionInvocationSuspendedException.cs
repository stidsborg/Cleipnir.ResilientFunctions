namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class FunctionInvocationSuspendedException : RFunctionException
{
    public FunctionId FunctionId { get; }

    public FunctionInvocationSuspendedException(FunctionId functionId)
        : base(
            functionId.TypeId,
            $"Function '{functionId}' invocation has been suspended"
        ) => FunctionId = functionId;
}
namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class ConcurrentModificationException : RFunctionException
{
    public FunctionId FunctionId { get; }

    public ConcurrentModificationException(FunctionId functionId)
        : base(
            functionId.TypeId,
            $"Unable to persist function '{functionId}' result due to concurrent modification"
        ) => FunctionId = functionId;
}
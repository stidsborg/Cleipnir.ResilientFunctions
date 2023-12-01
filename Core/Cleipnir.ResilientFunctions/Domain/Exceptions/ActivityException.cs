namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public class ActivityException : RFunctionException
{
    public FunctionId FunctionId { get; }
    public PreviouslyThrownException Exception { get; }

    public ActivityException(FunctionId functionId, string activityId, PreviouslyThrownException exception) 
        : base(functionId.TypeId, $"Activity '{activityId}' for function '{functionId}' failed")
    {
        FunctionId = functionId;
        Exception = exception;
    }
}
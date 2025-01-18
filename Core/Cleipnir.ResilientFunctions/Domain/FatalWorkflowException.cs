using System;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public abstract class FatalWorkflowException : Exception
{
    public FlowId FlowId { get; internal set; }
    public string FlowErrorMessage { get; }
    public string? FlowStackTrace { get; }
    public Type ErrorType { get; }
    
    public FatalWorkflowException(FlowId flowId, string errorMessage, string? stackTrace, Type errorType) : base(errorMessage)
    {
        FlowId = flowId;
        FlowErrorMessage = errorMessage;
        FlowStackTrace = stackTrace;
        ErrorType = errorType;
    }
    
    public static FatalWorkflowException Create(FlowId flowId, StoredException storedException)
    {
        var (message, stackTrace, exceptionTypeString) = storedException;
        var exceptionType = Type.GetType(exceptionTypeString, throwOnError: true);
        var genricFatalExceptionType = typeof(FatalWorkflowException<>).MakeGenericType(exceptionType!);
        return (FatalWorkflowException) Activator.CreateInstance(genricFatalExceptionType, args: [flowId, message, stackTrace, exceptionType])! 
               ?? throw new InvalidOperationException("Unable to create FatalWorkflowException from StoredException: " + storedException);
    }
    
    public static FatalWorkflowException<TException> Create<TException>(FlowId flowId, TException exception) where TException : Exception
        => new FatalWorkflowException<TException>(
            flowId,
            exception.Message,
            exception.StackTrace,
            typeof(TException)
        );

    public static FatalWorkflowException CreateNonGeneric(FlowId flowId, Exception exception)
    {
        var genricFatalExceptionType = typeof(FatalWorkflowException<>).MakeGenericType(exception.GetType());
        var message = exception.Message;
        var stackTrace = exception.StackTrace;
        var exceptionType = exception.GetType();
        
        return (FatalWorkflowException) Activator.CreateInstance(genricFatalExceptionType, args: [flowId, message, stackTrace, exceptionType])! 
               ?? throw new InvalidOperationException("Unable to create FatalWorkflowException from Exception: " + exception);
    }
}

public class FatalWorkflowException<TException>(FlowId flowId, string errorMessage, string? stackTrace, Type errorType)
    : FatalWorkflowException(flowId, errorMessage, stackTrace, errorType)
    where TException : Exception; 

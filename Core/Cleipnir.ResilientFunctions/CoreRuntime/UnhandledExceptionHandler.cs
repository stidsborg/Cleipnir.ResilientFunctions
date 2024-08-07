using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using static Cleipnir.ResilientFunctions.Helpers.ExceptionUtils;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class UnhandledExceptionHandler
{
    private readonly Action<FlowTypeException> _exceptionHandler;

    public UnhandledExceptionHandler(Action<FlowTypeException> exceptionHandler) 
        => _exceptionHandler = exceptionHandler;

    public void Invoke(FlowTypeException exception) => SafeTry(() => _exceptionHandler(exception));
    
    public void Invoke(FlowType flowType, Exception exception)
    {
        if (exception is FlowTypeException re)
            Invoke(re);
        else 
            Invoke(new FrameworkException(flowType, "Unhandled exception", exception));
    }
}
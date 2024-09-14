using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using static Cleipnir.ResilientFunctions.Helpers.ExceptionUtils;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class UnhandledExceptionHandler
{
    private readonly Action<FrameworkException> _exceptionHandler;

    public UnhandledExceptionHandler(Action<FrameworkException> exceptionHandler) 
        => _exceptionHandler = exceptionHandler;

    public void Invoke(FrameworkException exception) => SafeTry(() => _exceptionHandler(exception));
    
    public void Invoke(FlowType flowType, Exception exception)
    {
        if (exception is FrameworkException re)
            Invoke(re);
        else 
            Invoke(new FrameworkException($"Unhandled exception for flow '{flowType}'", exception));
    }
}
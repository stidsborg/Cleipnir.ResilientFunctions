using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using static Cleipnir.ResilientFunctions.Helpers.ExceptionUtils;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class UnhandledExceptionHandler
{
    private readonly Action<RFunctionException> _exceptionHandler;

    public UnhandledExceptionHandler(Action<RFunctionException> exceptionHandler) 
        => _exceptionHandler = exceptionHandler;

    public void Invoke(RFunctionException exception) => SafeTry(() => _exceptionHandler(exception));
    
    public void Invoke(FunctionTypeId functionTypeId, Exception exception)
    {
        if (exception is RFunctionException re)
            Invoke(re);
        else 
            Invoke(new FrameworkException(functionTypeId, "Unhandled exception", exception));
    }
}
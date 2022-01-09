using System;
using Cleipnir.ResilientFunctions.Domain;
using static Cleipnir.ResilientFunctions.Helpers.ExceptionUtils;

namespace Cleipnir.ResilientFunctions.ExceptionHandling;

public class UnhandledExceptionHandler
{
    private readonly Action<RFunctionException> _exceptionHandler;

    public UnhandledExceptionHandler(Action<RFunctionException> exceptionHandler) 
        => _exceptionHandler = exceptionHandler;

    public void Invoke(RFunctionException exception) => SafeTry(() => _exceptionHandler(exception));
}
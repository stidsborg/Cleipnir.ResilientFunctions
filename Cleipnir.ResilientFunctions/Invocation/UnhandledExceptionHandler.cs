using System;
using Cleipnir.ResilientFunctions.Domain;
using static Cleipnir.ResilientFunctions.Utils.ExceptionUtils;

namespace Cleipnir.ResilientFunctions.Invocation;

public class UnhandledExceptionHandler
{
    private readonly Action<RFunctionException> _exceptionHandler;

    public UnhandledExceptionHandler(Action<RFunctionException> exceptionHandler) 
        => _exceptionHandler = exceptionHandler;

    public void Invoke(RFunctionException exception) => SafeTry(() => _exceptionHandler(exception));
}
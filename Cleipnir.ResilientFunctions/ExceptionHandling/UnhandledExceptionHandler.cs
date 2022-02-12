using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using static Cleipnir.ResilientFunctions.Helpers.ExceptionUtils;

namespace Cleipnir.ResilientFunctions.ExceptionHandling;

public class UnhandledExceptionHandler
{
    private readonly Action<RFunctionException> _exceptionHandler;

    public UnhandledExceptionHandler(Action<RFunctionException> exceptionHandler) 
        => _exceptionHandler = exceptionHandler;

    public void Invoke(RFunctionException exception) => SafeTry(() => _exceptionHandler(exception));

    public void Invoke(Exception exception)
    {
        if (exception is RFunctionException re)
            Invoke(re);
        else 
            Invoke(new FrameworkException("Unhandled exception", exception));
    }

    public void InvokeIfCaught(Action action)
    {
        try
        {
            action();
        }
        catch (Exception exception)
        {
           Invoke(exception);
        }
    }
    
    public async Task InvokeIfCaught(Func<Task> action)
    {
        try
        {
            await action();
        }
        catch (Exception exception)
        {
            Invoke(exception);
        }
    }
}
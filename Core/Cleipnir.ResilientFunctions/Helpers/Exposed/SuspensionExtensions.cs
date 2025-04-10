using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;

namespace Cleipnir.ResilientFunctions.Helpers.Exposed;

public static class SuspensionExtensions
{
    public static async Task OnExceptionSuspendUntil(this Task task, DateTime until, Action<Exception>? onException = null)
    {
        try
        {
            await task;
        }
        catch (Exception ex)
        {
            onException?.Invoke(ex);
            throw new PostponeInvocationException(until);
        }
    }  
    
    public static async Task<T> OnExceptionSuspendUntil<T>(this Task<T> task, DateTime until, Action<Exception>? onException = null)
    {
        try
        {
            return await task;
        }
        catch (Exception ex)
        {
            onException?.Invoke(ex);
            throw new PostponeInvocationException(until);
        }
    }
    
    public static async ValueTask OnExceptionSuspendUntil(this ValueTask task, DateTime until, Action<Exception>? onException = null)
    {
        try
        {
            await task;
        }
        catch (Exception ex)
        {
            onException?.Invoke(ex);
            throw new PostponeInvocationException(until);
        }
    }  
    
    public static async ValueTask<T> OnExceptionSuspendUntil<T>(this ValueTask<T> task, DateTime until, Action<Exception>? onException = null)
    {
        try
        {
            return await task;
        }
        catch (Exception ex)
        {
            onException?.Invoke(ex);
            throw new PostponeInvocationException(until);
        }
    }
}
using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;

namespace Cleipnir.ResilientFunctions.Helpers.Exposed;

public static class SuspensionExtensions
{
    public static async Task OnExceptionSuspendFor(this Task task, TimeSpan duration, Action<Exception>? onException = null)
    {
        try
        {
            await task;
        }
        catch (Exception ex)
        {
            onException?.Invoke(ex);
            throw new PostponeInvocationException(duration);
        }
    }  
    
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
    
    public static async Task<T> OnExceptionSuspendFor<T>(this Task<T> task, TimeSpan duration, Action<Exception>? onException = null)
    {
        try
        {
            return await task;
        }
        catch (Exception ex)
        {
            onException?.Invoke(ex);
            throw new PostponeInvocationException(duration);
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
}
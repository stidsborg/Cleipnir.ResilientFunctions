using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.InnerDecorators;

public static class OnFailure
{
    public static Func<TParam, Task<Result>> PostponeFor<TParam>(Action<TParam> inner, int delayMs, Action<Exception>? onException = null)
    {
        return param =>
        {
            try
            {
                inner(param);
                return Succeed.WithoutValue.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delayMs).ToResult().ToTask();
            }
        };
    }
    
    public static Func<TParam, Task<Result>> PostponeFor<TParam>(Action<TParam> inner, TimeSpan delay, Action<Exception>? onException = null)
    {
        return param =>
        {
            try
            {
                inner(param);
                return Succeed.WithoutValue.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delay).ToResult().ToTask();
            }
        };
    }
    
    public static Func<TParam, Task<Result>> PostponeUntil<TParam>(
        Action<TParam> inner, 
        DateTime dateTime,
        Action<Exception>? onException = null)
    {
        return param =>
        {
            try
            {
                inner(param);
                return Succeed.WithoutValue.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.Until(dateTime).ToResult().ToTask();
            }
        };
    }
}
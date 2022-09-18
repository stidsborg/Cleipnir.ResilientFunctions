using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.InnerAdapters;

public static partial class OnFailure
{
    #region Func
    // sync with direct return
    public static Func<TParam, Task<Result<TReturn>>> PostponeFor<TParam, TReturn>(
        Func<TParam, TReturn> inner, 
        int delayMs, 
        Action<Exception>? onException = null)
    {
        return param =>
        {
            try
            {
                var returned = inner(param);
                return Succeed.WithValue(returned).ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delayMs).ToResult<TReturn>().ToTask();
            }
        };
    }
    
    public static Func<TParam, Task<Result<TReturn>>> PostponeFor<TParam, TReturn>(
        Func<TParam, TReturn> inner, 
        TimeSpan delay, 
        Action<Exception>? onException = null)
    {
        return param =>
        {
            try
            {
                var returned = inner(param);
                return Succeed.WithValue(returned).ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delay).ToResult<TReturn>().ToTask();
            }
        };
    }

    public static Func<TParam, Task<Result<TReturn>>> PostponeUntil<TParam, TReturn>(
        Func<TParam, TReturn> inner, 
        DateTime dateTime,
        Action<Exception>? onException = null)
    {
        return param =>
        {
            try
            {
                var returned = inner(param);
                return Succeed.WithValue(returned).ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.Until(dateTime).ToResult<TReturn>().ToTask();
            }
        };
    }
    
    //sync with result of return
    public static Func<TParam, Task<Result<TReturn>>> PostponeFor<TParam, TReturn>(
        Func<TParam, Result<TReturn>> inner, 
        int delayMs, 
        Action<Exception>? onException = null)
    {
        return param =>
        {
            try
            {
                var returned = inner(param);
                return returned.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delayMs).ToResult<TReturn>().ToTask();
            }
        };
    }
    
    public static Func<TParam, Task<Result<TReturn>>> PostponeFor<TParam, TReturn>(
        Func<TParam, Result<TReturn>> inner, 
        TimeSpan delay, 
        Action<Exception>? onException = null)
    {
        return param =>
        {
            try
            {
                var returned = inner(param);
                return returned.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delay).ToResult<TReturn>().ToTask();
            }
        };
    }

    public static Func<TParam, Task<Result<TReturn>>> PostponeUntil<TParam, TReturn>(
        Func<TParam, Result<TReturn>> inner, 
        DateTime dateTime,
        Action<Exception>? onException = null)
    {
        return param =>
        {
            try
            {
                var returned = inner(param);
                return returned.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.Until(dateTime).ToResult<TReturn>().ToTask();
            }
        };
    }
    
    //async with direct return
    public static Func<TParam, Task<Result<TReturn>>> PostponeFor<TParam, TReturn>(
        Func<TParam, Task<TReturn>> inner, 
        int delayMs, 
        Action<Exception>? onException = null)
    {
        return async param =>
        {
            try
            {
                var returned = await inner(param);
                return Succeed.WithValue(returned);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delayMs);
            }
        };
    }
    
    public static Func<TParam, Task<Result<TReturn>>> PostponeFor<TParam, TReturn>(
        Func<TParam, Task<TReturn>> inner, 
        TimeSpan delay, 
        Action<Exception>? onException = null)
    {
        return async param =>
        {
            try
            {
                var returned = await inner(param);
                return returned;
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delay);
            }
        };
    }
    
    public static Func<TParam, Task<Result<TReturn>>> PostponeUntil<TParam, TReturn>(
        Func<TParam, Task<TReturn>> inner, 
        DateTime dateTime,
        Action<Exception>? onException = null)
    {
        return async param =>
        {
            try
            {
                var returned = await inner(param);
                return returned;
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.Until(dateTime);
            }
        };
    }
    
    //async with result of return
    public static Func<TParam, Task<Result<TReturn>>> PostponeFor<TParam, TReturn>(
        Func<TParam, Task<Result<TReturn>>> inner, 
        int delayMs, 
        Action<Exception>? onException = null)
    {
        return async param =>
        {
            try
            {
                return await inner(param);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delayMs);
            }
        };
    }
    
    public static Func<TParam, Task<Result<TReturn>>> PostponeFor<TParam, TReturn>(
        Func<TParam, Task<Result<TReturn>>> inner, 
        TimeSpan delay, 
        Action<Exception>? onException = null)
    {
        return async param =>
        {
            try
            {
                return await inner(param);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delay);
            }
        };
    }
    
    public static Func<TParam, Task<Result<TReturn>>> PostponeUntil<TParam, TReturn>(
        Func<TParam, Task<Result<TReturn>>> inner, 
        DateTime dateTime,
        Action<Exception>? onException = null)
    {
        return async param =>
        {
            try
            {
                return await inner(param);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.Until(dateTime);
            }
        };
    }
    #endregion

    #region Action
    // sync with direct return
    public static Func<TParam, Task<Result>> PostponeFor<TParam>(
        Action<TParam> inner, 
        int delayMs, 
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
                return Postpone.For(delayMs).ToResult().ToTask();
            }
        };
    }
    
    public static Func<TParam, Task<Result>> PostponeFor<TParam>(
        Action<TParam> inner, 
        TimeSpan delay, 
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
    
    //sync with result of return
    public static Func<TParam, Task<Result>> PostponeFor<TParam>(
        Func<TParam, Result> inner, 
        int delayMs, 
        Action<Exception>? onException = null)
    {
        return param =>
        {
            try
            {
                var returned = inner(param);
                return returned.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delayMs).ToResult().ToTask();
            }
        };
    }
    
    public static Func<TParam, Task<Result>> PostponeFor<TParam>(
        Func<TParam, Result> inner, 
        TimeSpan delay, 
        Action<Exception>? onException = null)
    {
        return param =>
        {
            try
            {
                var returned = inner(param);
                return returned.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delay).ToResult().ToTask();
            }
        };
    }

    public static Func<TParam, Task<Result>> PostponeUntil<TParam>(
        Func<TParam, Result> inner, 
        DateTime dateTime,
        Action<Exception>? onException = null)
    {
        return param =>
        {
            try
            {
                var returned = inner(param);
                return returned.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.Until(dateTime).ToResult().ToTask();
            }
        };
    }
    
    //async with direct return
    public static Func<TParam, Task<Result>> PostponeFor<TParam>(
        Func<TParam, Task> inner, 
        int delayMs, 
        Action<Exception>? onException = null)
    {
        return async param =>
        {
            try
            {
                await inner(param);
                return Result.Succeed;
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delayMs);
            }
        };
    }
    
    public static Func<TParam, Task<Result>> PostponeFor<TParam>(
        Func<TParam, Task> inner, 
        TimeSpan delay, 
        Action<Exception>? onException = null)
    {
        return async param =>
        {
            try
            {
                await inner(param);
                return Result.Succeed;
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delay);
            }
        };
    }
    
    public static Func<TParam, Task<Result>> PostponeUntil<TParam>(
        Func<TParam, Task> inner, 
        DateTime dateTime,
        Action<Exception>? onException = null)
    {
        return async param =>
        {
            try
            {
                await inner(param);
                return Result.Succeed;
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.Until(dateTime);
            }
        };
    }
    
    //async with result of return
    public static Func<TParam, Task<Result>> PostponeFor<TParam>(
        Func<TParam, Task<Result>> inner, 
        int delayMs, 
        Action<Exception>? onException = null)
    {
        return async param =>
        {
            try
            {
                return await inner(param);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delayMs);
            }
        };
    }
    
    public static Func<TParam, Task<Result>> PostponeFor<TParam>(
        Func<TParam, Task<Result>> inner, 
        TimeSpan delay, 
        Action<Exception>? onException = null)
    {
        return async param =>
        {
            try
            {
                return await inner(param);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.For(delay);
            }
        };
    }
    
    public static Func<TParam, Task<Result>> PostponeUntil<TParam>(
        Func<TParam, Task<Result>> inner, 
        DateTime dateTime,
        Action<Exception>? onException = null)
    {
        return async param =>
        {
            try
            {
                return await inner(param);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception);
                return Postpone.Until(dateTime);
            }
        };
    }

    #endregion

    #region ActionWithScrapbook
    // sync with direct return
    public static Func<TParam, TScrapbook, Task<Result>> PostponeFor<TParam, TScrapbook>(
        Action<TParam, TScrapbook> inner, 
        int delayMs, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return (param, scrapbook) =>
        {
            try
            {
                inner(param, scrapbook);
                return Succeed.WithoutValue.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delayMs).ToResult().ToTask();
            }
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result>> PostponeFor<TParam, TScrapbook>(
        Action<TParam, TScrapbook> inner, 
        TimeSpan delay, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return (param, scrapbook) =>
        {
            try
            {
                inner(param, scrapbook);
                return Succeed.WithoutValue.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delay).ToResult().ToTask();
            }
        };
    }

    public static Func<TParam, TScrapbook, Task<Result>> PostponeUntil<TParam, TScrapbook>(
        Action<TParam, TScrapbook> inner, 
        DateTime dateTime,
        Action<Exception, TScrapbook>? onException = null)
    {
        return (param, scrapbook) =>
        {
            try
            {
                inner(param, scrapbook);
                return Succeed.WithoutValue.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.Until(dateTime).ToResult().ToTask();
            }
        };
    }
    
    //sync with result of return
    public static Func<TParam, TScrapbook, Task<Result>> PostponeFor<TParam, TScrapbook>(
        Func<TParam, TScrapbook, Result> inner, 
        int delayMs, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return (param, scrapbook) =>
        {
            try
            {
                var returned = inner(param, scrapbook);
                return returned.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delayMs).ToResult().ToTask();
            }
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result>> PostponeFor<TParam, TScrapbook>(
        Func<TParam, TScrapbook, Result> inner, 
        TimeSpan delay, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return (param, scrapbook) =>
        {
            try
            {
                var returned = inner(param, scrapbook);
                return returned.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delay).ToResult().ToTask();
            }
        };
    }

    public static Func<TParam, TScrapbook, Task<Result>> PostponeUntil<TParam, TScrapbook>(
        Func<TParam, TScrapbook, Result> inner, 
        DateTime dateTime,
        Action<Exception, TScrapbook>? onException = null)
    {
        return (param, scrapbook) =>
        {
            try
            {
                var returned = inner(param, scrapbook);
                return returned.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.Until(dateTime).ToResult().ToTask();
            }
        };
    }
    
    //async with direct return
    public static Func<TParam, TScrapbook, Task<Result>> PostponeFor<TParam, TScrapbook>(
        Func<TParam, TScrapbook, Task> inner, 
        int delayMs, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return async (param, scrapbook) =>
        {
            try
            {
                await inner(param, scrapbook);
                return Result.Succeed;
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delayMs);
            }
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result>> PostponeFor<TParam, TScrapbook>(
        Func<TParam, TScrapbook, Task> inner, 
        TimeSpan delay, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return async (param, scrapbook) =>
        {
            try
            {
                await inner(param, scrapbook);
                return Result.Succeed;
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delay);
            }
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result>> PostponeUntil<TParam, TScrapbook>(
        Func<TParam, TScrapbook, Task> inner, 
        DateTime dateTime,
        Action<Exception, TScrapbook>? onException = null)
    {
        return async (param, scrapbook) =>
        {
            try
            {
                await inner(param, scrapbook);
                return Result.Succeed;
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.Until(dateTime);
            }
        };
    }
    
    //async with result of return
    public static Func<TParam, TScrapbook, Task<Result>> PostponeFor<TParam, TScrapbook>(
        Func<TParam, TScrapbook, Task<Result>> inner, 
        int delayMs, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return async (param, scrapbook) =>
        {
            try
            {
                return await inner(param, scrapbook);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delayMs);
            }
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result>> PostponeFor<TParam, TScrapbook>(
        Func<TParam, TScrapbook, Task<Result>> inner, 
        TimeSpan delay, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return async (param, scrapbook) =>
        {
            try
            {
                return await inner(param, scrapbook);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delay);
            }
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result>> PostponeUntil<TParam, TScrapbook>(
        Func<TParam, TScrapbook, Task<Result>> inner, 
        DateTime dateTime,
        Action<Exception, TScrapbook>? onException = null)
    {
        return async (param, scrapbook) =>
        {
            try
            {
                return await inner(param, scrapbook);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.Until(dateTime);
            }
        };
    }
    #endregion

    #region FuncWithScrapbook
    // sync with direct return
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> PostponeFor<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, TReturn> inner, 
        int delayMs, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return (param, scrapbook) =>
        {
            try
            {
                var returned = inner(param, scrapbook);
                return Succeed.WithValue(returned).ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delayMs).ToResult<TReturn>().ToTask();
            }
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> PostponeFor<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, TReturn> inner, 
        TimeSpan delay, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return (param, scrapbook) =>
        {
            try
            {
                var returned = inner(param, scrapbook);
                return Succeed.WithValue(returned).ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delay).ToResult<TReturn>().ToTask();
            }
        };
    }

    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> PostponeUntil<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, TReturn> inner, 
        DateTime dateTime,
        Action<Exception, TScrapbook>? onException = null)
    {
        return (param, scrapbook) =>
        {
            try
            {
                var returned = inner(param, scrapbook);
                return Succeed.WithValue(returned).ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.Until(dateTime).ToResult<TReturn>().ToTask();
            }
        };
    }
    
    //sync with result of return
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> PostponeFor<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Result<TReturn>> inner, 
        int delayMs, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return (param, scrapbook) =>
        {
            try
            {
                var returned = inner(param, scrapbook);
                return returned.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delayMs).ToResult<TReturn>().ToTask();
            }
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> PostponeFor<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Result<TReturn>> inner, 
        TimeSpan delay, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return (param, scrapbook) =>
        {
            try
            {
                var returned = inner(param, scrapbook);
                return returned.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delay).ToResult<TReturn>().ToTask();
            }
        };
    }

    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> PostponeUntil<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Result<TReturn>> inner, 
        DateTime dateTime,
        Action<Exception, TScrapbook>? onException = null)
    {
        return (param, scrapbook) =>
        {
            try
            {
                var returned = inner(param, scrapbook);
                return returned.ToTask();
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.Until(dateTime).ToResult<TReturn>().ToTask();
            }
        };
    }
    
    //async with direct return
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> PostponeFor<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<TReturn>> inner, 
        int delayMs, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return async (param, scrapbook) =>
        {
            try
            {
                var returned = await inner(param, scrapbook);
                return Succeed.WithValue(returned);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delayMs);
            }
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> PostponeFor<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<TReturn>> inner, 
        TimeSpan delay, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return async (param, scrapbook) =>
        {
            try
            {
                var returned = await inner(param, scrapbook);
                return returned;
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delay);
            }
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> PostponeUntil<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<TReturn>> inner, 
        DateTime dateTime,
        Action<Exception, TScrapbook>? onException = null)
    {
        return async (param, scrapbook) =>
        {
            try
            {
                var returned = await inner(param, scrapbook);
                return returned;
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.Until(dateTime);
            }
        };
    }
    
    //async with result of return
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> PostponeFor<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner, 
        int delayMs, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return async (param, scrapbook) =>
        {
            try
            {
                return await inner(param, scrapbook);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delayMs);
            }
        };
    }
    
    public static Func<TParam,  TScrapbook,Task<Result<TReturn>>> PostponeFor<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner, 
        TimeSpan delay, 
        Action<Exception, TScrapbook>? onException = null)
    {
        return async (param, scrapbook) =>
        {
            try
            {
                return await inner(param, scrapbook);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.For(delay);
            }
        };
    }
    
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> PostponeUntil<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner, 
        DateTime dateTime,
        Action<Exception, TScrapbook>? onException = null)
    {
        return async (param, scrapbook) =>
        {
            try
            {
                return await inner(param, scrapbook);
            }
            catch (Exception exception)
            {
                onException?.Invoke(exception, scrapbook);
                return Postpone.Until(dateTime);
            }
        };
    }
    #endregion
}
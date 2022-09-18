using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Utils.Scrapbooks;

namespace Cleipnir.ResilientFunctions.InnerAdapters;

public static partial class OnFailure
{
    #region Func
    // sync with direct return
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> BackoffExponentially<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, TReturn> inner, 
        TimeSpan firstDelay,
        double factor,
        int maxRetries,
        Action<Exception, TScrapbook>? onException = null
    ) where TScrapbook : IBackoffScrapbook, new()
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
                if (scrapbook.Retry == maxRetries) throw;
                
                var delay = firstDelay * Math.Pow(factor, scrapbook.Retry);
                scrapbook.Retry++;
                return Postpone.For(delay).ToResult<TReturn>().ToTask();
            }
        };
    }
    
    //sync with result of return
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> BackoffExponentially<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Result<TReturn>> inner, 
        TimeSpan firstDelay,
        double factor,
        int maxRetries,
        Action<Exception, TScrapbook>? onException = null
    ) where TScrapbook : IBackoffScrapbook, new()
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
                if (scrapbook.Retry == maxRetries) throw;
                
                var delay = firstDelay * Math.Pow(factor, scrapbook.Retry);
                scrapbook.Retry++;
                return Postpone.For(delay).ToResult<TReturn>().ToTask();
            }
        };
    }
    
    //async with direct return
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> BackoffExponentially<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<TReturn>> inner, 
        TimeSpan firstDelay,
        double factor,
        int maxRetries,
        Action<Exception, TScrapbook>? onException = null
    ) where TScrapbook : IBackoffScrapbook, new()
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
                if (scrapbook.Retry == maxRetries) throw;
                
                var delay = firstDelay * Math.Pow(factor, scrapbook.Retry);
                scrapbook.Retry++;
                return Postpone.For(delay);
            }
        };
    }
    
    //async with result of return
    public static Func<TParam, TScrapbook, Task<Result<TReturn>>> BackoffExponentially<TParam, TScrapbook, TReturn>(
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner, 
        TimeSpan firstDelay,
        double factor,
        int maxRetries,
        Action<Exception, TScrapbook>? onException = null
    ) where TScrapbook : IBackoffScrapbook, new()
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
                if (scrapbook.Retry == maxRetries) throw;
                
                var delay = firstDelay * Math.Pow(factor, scrapbook.Retry);
                scrapbook.Retry++;
                return Postpone.For(delay);
            }
        };
    }
    #endregion

    #region Action
    // sync with direct return
    public static Func<TParam, TScrapbook, Task<Result>> BackoffExponentially<TParam, TScrapbook>(
        Action<TParam, TScrapbook> inner, 
        TimeSpan firstDelay,
        double factor,
        int maxRetries,
        Action<Exception, TScrapbook>? onException = null
    ) where TScrapbook : IBackoffScrapbook, new()
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
                if (scrapbook.Retry == maxRetries) throw;
                
                var delay = firstDelay * Math.Pow(factor, scrapbook.Retry);
                scrapbook.Retry++;
                return Postpone.For(delay).ToResult().ToTask();
            }
        };
    }
    
    //sync with result of return
    public static Func<TParam, TScrapbook, Task<Result>> BackoffExponentially<TParam, TScrapbook>(
        Func<TParam, TScrapbook, Result> inner, 
        TimeSpan firstDelay,
        double factor,
        int maxRetries,
        Action<Exception, TScrapbook>? onException = null
    ) where TScrapbook : IBackoffScrapbook, new()
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
                if (scrapbook.Retry == maxRetries) throw;
                
                var delay = firstDelay * Math.Pow(factor, scrapbook.Retry);
                scrapbook.Retry++;
                return Postpone.For(delay).ToResult().ToTask();
            }
        };
    }
    
    //async with direct return
    public static Func<TParam, TScrapbook, Task<Result>> BackoffExponentially<TParam, TScrapbook>(
        Func<TParam, TScrapbook, Task> inner, 
        TimeSpan firstDelay,
        double factor,
        int maxRetries,
        Action<Exception, TScrapbook>? onException = null
    ) where TScrapbook : IBackoffScrapbook, new()
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
                if (scrapbook.Retry == maxRetries) throw;
                
                var delay = firstDelay * Math.Pow(factor, scrapbook.Retry);
                scrapbook.Retry++;
                return Postpone.For(delay);
            }
        };
    }

    //async with result of return
    public static Func<TParam, TScrapbook, Task<Result>> BackoffExponentially<TParam, TScrapbook>(
        Func<TParam, TScrapbook, Task<Result>> inner,
        TimeSpan firstDelay,
        double factor,
        int maxRetries,
        Action<Exception, TScrapbook>? onException = null
    ) where TScrapbook : IBackoffScrapbook, new()
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
                if (scrapbook.Retry == maxRetries) throw;
                
                var delay = firstDelay * Math.Pow(factor, scrapbook.Retry);
                scrapbook.Retry++;
                return Postpone.For(delay);
            }
        };
    }
    #endregion
}
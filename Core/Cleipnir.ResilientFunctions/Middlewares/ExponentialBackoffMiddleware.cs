using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Middlewares;

public class ExponentialBackoffMiddleware : IMiddleware
{
    private readonly TimeSpan _firstDelay;
    private readonly double _factor;
    private readonly int _maxTries;
    private readonly TimeSpan _inMemoryThreshold;
    private readonly Action<Exception>? _onException;

    public ExponentialBackoffMiddleware(
        TimeSpan firstDelay, 
        double factor, 
        int maxTries, 
        TimeSpan inMemoryThreshold,
        Action<Exception>? onException = null)
    {
        _firstDelay = firstDelay;
        _factor = factor;
        _maxTries = maxTries;
        _inMemoryThreshold = inMemoryThreshold;
        _onException = onException;
    }

    public async Task<Result<TResult>> Invoke<TParam, TScrapbook, TResult>(
        TParam param,
        TScrapbook scrapbook,
        Context context,
        Func<TParam, TScrapbook, Context, Task<Result<TResult>>> next
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        while (true)
        {
            var result =  await next(param, scrapbook, context);
            if (result.Outcome is not Outcome.Fail) return result;

            var failedWithException = result.Fail!;
            _onException?.Invoke(failedWithException);
                
            if (!scrapbook.StateDictionary.ContainsKey($"{nameof(ExponentialBackoffMiddleware)}.RetriesSoFar"))
                scrapbook.StateDictionary[$"{nameof(ExponentialBackoffMiddleware)}.RetriesSoFar"] = "0";

            var retriesSoFar = int.Parse(scrapbook.StateDictionary[$"{nameof(ExponentialBackoffMiddleware)}.RetriesSoFar"]);
            retriesSoFar++;
            scrapbook.StateDictionary[$"{nameof(ExponentialBackoffMiddleware)}.RetriesSoFar"] = retriesSoFar.ToString();

            if (retriesSoFar >= _maxTries)
                return result;

            var delay = _firstDelay * Math.Pow(_factor, retriesSoFar);
            if (delay >= _inMemoryThreshold)
                return Postpone.For(delay);

            await Task.Delay(delay);
        }
    }
}
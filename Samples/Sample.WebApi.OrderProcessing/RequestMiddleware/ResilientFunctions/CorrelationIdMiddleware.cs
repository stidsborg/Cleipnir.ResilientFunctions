using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;

namespace Sample.WebApi.OrderProcessing.RequestMiddleware.ResilientFunctions;

public class CorrelationIdMiddleware : IPreCreationMiddleware
{
    private readonly CorrelationId _correlationId;

    public CorrelationIdMiddleware(CorrelationId correlationId) => _correlationId = correlationId;

    public Task<Result<TResult>> Invoke<TParam, TScrapbook, TResult>(
        TParam param, 
        TScrapbook scrapbook, 
        Context context, 
        Func<TParam, TScrapbook, Context, Task<Result<TResult>>> next
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        if (context.InvocationMode == InvocationMode.Retry)
            _correlationId.Value = scrapbook.StateDictionary["CorrelationId"];
        
        return next(param, scrapbook, context);
    }

    public Task PreCreation<TParam>(TParam param, Dictionary<string, string> stateDictionary, FunctionId functionId) where TParam : notnull
    {
        stateDictionary["CorrelationId"] = _correlationId.Value;
        return Task.CompletedTask;
    }
}
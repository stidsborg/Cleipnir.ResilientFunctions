using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Invocation;
using Serilog.Context;

namespace Sample.WebApi.OrderProcessing.RequestMiddleware.ResilientFunctions;

public class LoggingMiddleware : Cleipnir.ResilientFunctions.Invocation.IMiddleware
{
    private readonly CorrelationId _correlationId;

    public LoggingMiddleware(CorrelationId correlationId) => _correlationId = correlationId;

    public async Task<Result<TResult>> Invoke<TParam, TScrapbook, TResult>(
        TParam param, 
        TScrapbook scrapbook, 
        Context context, 
        Func<TParam, TScrapbook, Context, Task<Result<TResult>>> next
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        var correlationId = _correlationId.Value;
        using var _ = LogContext.PushProperty("CorrelationId", correlationId);
        return await next(param, scrapbook, context);
    }
}
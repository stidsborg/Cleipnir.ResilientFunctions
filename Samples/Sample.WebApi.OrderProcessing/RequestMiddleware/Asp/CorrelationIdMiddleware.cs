namespace Sample.WebApi.OrderProcessing.RequestMiddleware.Asp;

public class CorrelationIdMiddleware : IMiddleware
{
    private readonly CorrelationId _correlationId;

    public CorrelationIdMiddleware(CorrelationId correlationId) => _correlationId = correlationId;

    public Task InvokeAsync(HttpContext context, RequestDelegate next)
    {
        _correlationId.Value = Guid.NewGuid().ToString();
        return next(context);
    }
}
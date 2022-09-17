namespace Sample.WebApi.OrderProcessing.RequestMiddleware;

public class CorrelationId
{
    private readonly AsyncLocal<string> _correlationId = new();

    public string Value
    {
        get => _correlationId.Value!;
        set => _correlationId.Value = value;
    }
}
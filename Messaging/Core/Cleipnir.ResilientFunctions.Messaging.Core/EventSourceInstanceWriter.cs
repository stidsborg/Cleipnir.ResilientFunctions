using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class EventSourceInstanceWriter
{
    private readonly FunctionInstanceId _functionInstanceId;
    private readonly EventSourceWriter _writer;

    public EventSourceInstanceWriter(FunctionInstanceId functionInstanceId, EventSourceWriter writer)
    {
        _functionInstanceId = functionInstanceId;
        _writer = writer;
    }

    public Task Append(object @event, string? idempotencyKey, bool awakeIfSuspended)
        => _writer.Append(_functionInstanceId, @event, idempotencyKey, awakeIfSuspended);

    public Task Append(IEnumerable<EventAndIdempotencyKey> events, bool awakeIfSuspended)
        => _writer.Append(_functionInstanceId, events, awakeIfSuspended);

    public Task Truncate() => _writer.Truncate(_functionInstanceId);
}
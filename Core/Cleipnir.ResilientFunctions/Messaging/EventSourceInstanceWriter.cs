using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public class EventSourceInstanceWriter
{
    private readonly FunctionInstanceId _functionInstanceId;
    private readonly EventSourceWriter _writer;

    public EventSourceInstanceWriter(FunctionInstanceId functionInstanceId, EventSourceWriter writer)
    {
        _functionInstanceId = functionInstanceId;
        _writer = writer;
    }

    public Task Append(object @event, string? idempotencyKey = null, bool awakeIfSuspended = false)
        => _writer.Append(_functionInstanceId, @event, idempotencyKey, awakeIfSuspended);

    public Task Append(IEnumerable<EventAndIdempotencyKey> events, bool awakeIfSuspended = false)
        => _writer.Append(_functionInstanceId, events, awakeIfSuspended);

    public Task Truncate() => _writer.Truncate(_functionInstanceId);
}
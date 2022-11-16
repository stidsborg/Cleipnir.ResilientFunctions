using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public class EventSourceWriters
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly IEventStore _eventStore;
    private readonly ISerializer _serializer;

    public EventSourceWriters(FunctionTypeId functionTypeId, IEventStore eventStore, ISerializer serializer)
    {
        _functionTypeId = functionTypeId;
        _eventStore = eventStore;
        _serializer = serializer;
    }

    public EventSourceWriter For(FunctionInstanceId instanceId)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        return new EventSourceWriter(functionId, _eventStore, _serializer);
    }
}
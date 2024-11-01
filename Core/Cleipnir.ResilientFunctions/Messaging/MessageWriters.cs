using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriters
{
    private readonly FlowType _flowType;
    private readonly StoredType _storedType;
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly ScheduleReInvocation _scheduleReInvocation;

    public MessageWriters(
        FlowType flowType, 
        StoredType storedType,
        IFunctionStore functionStore, 
        ISerializer serializer, 
        ScheduleReInvocation scheduleReInvocation)
    {
        _flowType = flowType;
        _storedType = storedType;
        _functionStore = functionStore;
        _serializer = serializer;
        _scheduleReInvocation = scheduleReInvocation;
    }

    public MessageWriter For(FlowInstance instance)
    {
        var storedId = new StoredId(_storedType, instance.Value);
        return new MessageWriter(storedId, _functionStore, _serializer, _scheduleReInvocation);
    }
}
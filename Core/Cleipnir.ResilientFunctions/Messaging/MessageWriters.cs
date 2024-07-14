using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriters
{
    private readonly FlowType _flowType;
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly ScheduleReInvocation _scheduleReInvocation;

    public MessageWriters(
        FlowType flowType, 
        IFunctionStore functionStore, 
        ISerializer serializer, 
        ScheduleReInvocation scheduleReInvocation)
    {
        _flowType = flowType;
        _functionStore = functionStore;
        _serializer = serializer;
        _scheduleReInvocation = scheduleReInvocation;
    }

    public MessageWriter For(FlowInstance instance)
    {
        var functionId = new FlowId(_flowType, instance);
        return new MessageWriter(functionId, _functionStore, _serializer, _scheduleReInvocation);
    }
}
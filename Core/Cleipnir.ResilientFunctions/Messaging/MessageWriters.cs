using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriters
{
    private readonly StoredType _storedType;
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly ScheduleReInvocation _scheduleReInvocation;

    public MessageWriters(
        StoredType storedType,
        IFunctionStore functionStore, 
        ISerializer serializer, 
        ScheduleReInvocation scheduleReInvocation)
    {
        _storedType = storedType;
        _functionStore = functionStore;
        _serializer = serializer;
        _scheduleReInvocation = scheduleReInvocation;
    }

    public MessageWriter For(FlowInstance instance)
    {
        var storedId = new StoredId(_storedType, instance.Value.ToStoredInstance());
        return new MessageWriter(storedId, _functionStore, _serializer, _scheduleReInvocation);
    }
    
    internal MessageWriter For(StoredInstance instance)
    {
        var storedId = new StoredId(_storedType, instance);
        return new MessageWriter(storedId, _functionStore, _serializer, _scheduleReInvocation);
    }
}
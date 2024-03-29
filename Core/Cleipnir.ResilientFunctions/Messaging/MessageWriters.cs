﻿using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriters
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly ScheduleReInvocation _scheduleReInvocation;

    public MessageWriters(
        FunctionTypeId functionTypeId, 
        IFunctionStore functionStore, 
        ISerializer serializer, 
        ScheduleReInvocation scheduleReInvocation)
    {
        _functionTypeId = functionTypeId;
        _functionStore = functionStore;
        _serializer = serializer;
        _scheduleReInvocation = scheduleReInvocation;
    }

    public MessageWriter For(FunctionInstanceId instanceId)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        return new MessageWriter(functionId, _functionStore, _serializer, _scheduleReInvocation);
    }
}
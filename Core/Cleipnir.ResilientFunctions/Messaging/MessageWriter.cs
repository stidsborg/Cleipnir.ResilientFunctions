using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriter
{
    private readonly FunctionId _functionId;
    private readonly IFunctionStore _functionStore;
    private readonly IMessageStore _messageStore;
    private readonly ISerializer _serializer;
    private readonly ScheduleReInvocation _scheduleReInvocation;

    public MessageWriter(FunctionId functionId, IFunctionStore functionStore, ISerializer eventSerializer, ScheduleReInvocation scheduleReInvocation)
    {
        _functionId = functionId;
        _functionStore = functionStore;
        _messageStore = functionStore.MessageStore;
        _serializer = eventSerializer;
        _scheduleReInvocation = scheduleReInvocation;
    }

    public async Task AppendMessage<TEvent>(TEvent @event, string? idempotencyKey = null) where TEvent : notnull
    {
        var (eventJson, eventType) = _serializer.SerializeMessage(@event);
        await _messageStore.AppendMessage(
            _functionId,
            eventJson,
            eventType,
            idempotencyKey
        );

        var statusAndEpoch = await _functionStore.GetFunctionStatus(_functionId);
        if (statusAndEpoch == null)
            throw new UnexpectedFunctionState(_functionId, $"Function '{_functionId}' not found");

        var (status, epoch) = statusAndEpoch;
        if (status == Status.Suspended || status == Status.Postponed)
        {
            try
            {
                await _scheduleReInvocation(_functionId.InstanceId.Value, expectedEpoch: epoch);
            }
            catch (UnexpectedFunctionState)
            {

            }
        }
            
    }
    
    public async Task AppendMessage(MessageAndIdempotencyKey messageAndIdempotency)
    {
        var (@event, idempotencyKey) = messageAndIdempotency;
        var (eventJson, eventType) = _serializer.SerializeMessage(@event);
        await _messageStore.AppendMessage(
            _functionId,
            eventJson,
            eventType,
            idempotencyKey
        );

        var statusAndEpoch = await _functionStore.GetFunctionStatus(_functionId);
        if (statusAndEpoch == null)
            throw new UnexpectedFunctionState(_functionId, $"Function '{_functionId}' not found");

        var (status, epoch) = statusAndEpoch;
        if (status == Status.Suspended || status == Status.Postponed)
            await _scheduleReInvocation(_functionId.InstanceId.Value, expectedEpoch: epoch);
    }

    public Task Truncate() => _messageStore.Truncate(_functionId);
}
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
        var (status, epoch) = await _messageStore.AppendMessage(
            _functionId,
            new StoredMessage(eventJson, eventType, idempotencyKey)
        );

        if (status == Status.Failed || status == Status.Succeeded)
            return;
        
        if (status == Status.Executing)
        {
            var success = await _functionStore.IncrementInterruptCount(_functionId);
            if (success)
                return; //executing function will notice interrupt increment and reschedule itself on suspension
            
            //otherwise update status and epoch so we can reschedule if new status is postponed or suspended
            var statusAndEpoch = await _functionStore.GetFunctionStatus(_functionId);
            if (statusAndEpoch == null)
                return;

            status = statusAndEpoch.Status;
            epoch = statusAndEpoch.Epoch;
        }

        if (status == Status.Postponed || status == Status.Suspended)
            try 
            {
                await _scheduleReInvocation(_functionId.InstanceId.Value, expectedEpoch: epoch);
            }
            catch (UnexpectedFunctionState) { }
    }
    
    public Task Truncate() => _messageStore.Truncate(_functionId);
}
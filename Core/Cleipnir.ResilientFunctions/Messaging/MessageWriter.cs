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

    public async Task<Finding> AppendMessage<TEvent>(TEvent @event, string? idempotencyKey = null) where TEvent : notnull
    {
        var (eventJson, eventType) = _serializer.SerializeMessage(@event);
        
        var functionStatus = await _messageStore.AppendMessage(
            _functionId,
            new StoredMessage(eventJson, eventType, idempotencyKey)
        );
        if (functionStatus == null)
            return Finding.NotFound;
        
        var (status, epoch) = functionStatus;
        if (status == Status.Failed || status == Status.Succeeded)
            return Finding.Found;
        
        if (status == Status.Executing)
        {
            var success = await _functionStore.IncrementInterruptCount(_functionId);
            if (success)
                return Finding.Found; //executing function will notice interrupt increment and reschedule itself on suspension
            
            //otherwise update status and epoch, so we can reschedule if new status is postponed or suspended
            var statusAndEpoch = await _functionStore.GetFunctionStatus(_functionId);
            if (statusAndEpoch == null)
                return Finding.Found;

            status = statusAndEpoch.Status;
            epoch = statusAndEpoch.Epoch;
        }

        if (status == Status.Postponed || status == Status.Suspended)
            try 
            {
                await _scheduleReInvocation(_functionId.InstanceId.Value, expectedEpoch: epoch);
            }
            catch (UnexpectedFunctionState) { }

        return Finding.Found;
    }
}
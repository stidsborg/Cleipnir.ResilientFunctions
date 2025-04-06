using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriter
{
    private readonly StoredId _storedId;
    private readonly IFunctionStore _functionStore;
    private readonly IMessageStore _messageStore;
    private readonly ISerializer _serializer;
    private readonly ScheduleReInvocation _scheduleReInvocation;

    public MessageWriter(StoredId storedIdId, IFunctionStore functionStore, ISerializer eventSerializer, ScheduleReInvocation scheduleReInvocation)
    {
        _storedId = storedIdId;
        _functionStore = functionStore;
        _messageStore = functionStore.MessageStore;
        _serializer = eventSerializer;
        _scheduleReInvocation = scheduleReInvocation;
    }

    public async Task<Finding> AppendMessage<TMessage>(TMessage message, string? idempotencyKey = null) where TMessage : notnull
    {
        var (eventJson, eventType) = _serializer.SerializeMessage(message, typeof(TMessage));
        
        var functionStatus = await _messageStore.AppendMessage(
            _storedId,
            new StoredMessage(eventJson, eventType, idempotencyKey)
        );
        if (functionStatus == null)
            return Finding.NotFound;
        
        var (status, epoch) = functionStatus;
        if (status == Status.Failed || status == Status.Succeeded)
            return Finding.Found;
        
        if (status == Status.Executing)
        {
            var success = await _functionStore.Interrupt(_storedId, onlyIfExecuting: true);
            if (success)
                return Finding.Found; //executing function will notice interrupt increment and reschedule itself on suspension
            
            //otherwise update status and epoch, so we can reschedule if new status is postponed or suspended
            var statusAndEpoch = await _functionStore.GetFunctionStatus(_storedId);
            if (statusAndEpoch == null)
                return Finding.Found;

            status = statusAndEpoch.Status;
            epoch = statusAndEpoch.Epoch;
        }

        if (status == Status.Postponed || status == Status.Suspended)
            try 
            {
                await _scheduleReInvocation(_storedId.Instance, expectedEpoch: epoch);
            }
            catch (UnexpectedStateException) { }

        return Finding.Found;
    }

    internal async Task AppendMessageNoSync<TMessage>(TMessage message, string? idempotencyKey = null) where TMessage : notnull
    {
        var (eventJson, eventType) = _serializer.SerializeMessage(message, typeof(TMessage));
        await _messageStore.AppendMessageNoStatusAndInterrupt(
            _storedId,
            new StoredMessage(eventJson, eventType, idempotencyKey)
        );
    }
}
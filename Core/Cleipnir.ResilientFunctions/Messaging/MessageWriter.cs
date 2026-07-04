using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriter
{
    private readonly StoredId _storedId;
    private readonly IMessageStore _messageStore;
    private readonly ISerializer _eventSerializer;
    private readonly ReplicaId _publisherReplica;
    private readonly MessageWatchdog? _messageWatchdog;

    internal MessageWriter(StoredId storedId, IMessageStore messageStore, ISerializer eventSerializer, ReplicaId publisherReplica, MessageWatchdog? messageWatchdog = null)
    {
        _storedId = storedId;
        _messageStore = messageStore;
        _eventSerializer = eventSerializer;
        _publisherReplica = publisherReplica;
        _messageWatchdog = messageWatchdog;
    }

    public async Task AppendMessage<TMessage>(TMessage message, string? idempotencyKey = null, string? sender = null, string? receiver = null) where TMessage : class
    {
        var eventJson = _eventSerializer.Serialize(message, message.GetType());
        var eventType = _eventSerializer.SerializeType(message.GetType());

        var storedMessage = new StoredMessage(eventJson, eventType, Position: 0, Replica: _publisherReplica, IdempotencyKey: idempotencyKey, Sender: sender, Receiver: receiver);
        await _messageStore.AppendMessages([new StoredIdAndMessage(_storedId, storedMessage)]);

        // Wake the MessageWatchdog so the appended message is delivered now rather than on the next poll.
        _messageWatchdog?.Notify();
    }
}

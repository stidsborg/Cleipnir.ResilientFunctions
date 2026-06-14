using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriter(StoredId storedIdId, IMessageStore messageStore, ISerializer eventSerializer, ReplicaId publisherReplica)
{
    public async Task AppendMessage<TMessage>(TMessage message, string? idempotencyKey = null, string? sender = null, string? receiver = null) where TMessage : class
    {
        var eventJson = eventSerializer.Serialize(message, message.GetType());
        var eventType = eventSerializer.SerializeType(message.GetType());

        var storedMessage = new StoredMessage(eventJson, eventType, Position: 0, Replica: publisherReplica, IdempotencyKey: idempotencyKey, Sender: sender, Receiver: receiver);
        await messageStore.AppendMessages([new StoredIdAndMessage(storedIdId, storedMessage)]);
    }
}
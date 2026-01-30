using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriter(StoredId storedIdId, IMessageStore messageStore, ISerializer eventSerializer)
{
    public async Task AppendMessage<TMessage>(TMessage message, string? idempotencyKey = null, string? sender = null) where TMessage : class
    {
        var eventJson = eventSerializer.Serialize(message, message.GetType());
        var eventType = eventSerializer.SerializeType(message.GetType());

         await messageStore.AppendMessage(
            storedIdId,
            new StoredMessage(eventJson, eventType, Position: 0, idempotencyKey, Sender: sender)
        );
    }
}
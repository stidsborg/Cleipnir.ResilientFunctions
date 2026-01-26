using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriter(StoredId storedIdId, IMessageStore messageStore, ISerializer eventSerializer)
{
    public async Task AppendMessage<TMessage>(TMessage message, string? idempotencyKey = null) where TMessage : class
    {
        eventSerializer.Serialize(message, out var eventJson, out var eventType);

         await messageStore.AppendMessage(
            storedIdId,
            new StoredMessage(eventJson, eventType, Position: 0, idempotencyKey)
        );
    }
}
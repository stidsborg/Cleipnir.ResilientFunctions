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

    public MessageWriter(StoredId storedIdId, IFunctionStore functionStore, ISerializer eventSerializer)
    {
        _storedId = storedIdId;
        _functionStore = functionStore;
        _messageStore = functionStore.MessageStore;
        _serializer = eventSerializer;
    }

    public async Task AppendMessage<TMessage>(TMessage message, string? idempotencyKey = null) where TMessage : notnull
    {
        var (eventJson, eventType) = _serializer.SerializeMessage(message, typeof(TMessage));
        
         await _messageStore.AppendMessage(
            _storedId,
            new StoredMessage(eventJson, eventType, idempotencyKey)
        );
        await _functionStore.Interrupt(_storedId);
    }
}
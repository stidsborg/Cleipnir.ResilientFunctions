using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriter
{
    private readonly StoredId _storedId;
    private readonly MessagesSender _messagesSender;

    internal MessageWriter(StoredId storedId, MessagesSender messagesSender)
    {
        _storedId = storedId;
        _messagesSender = messagesSender;
    }

    public async Task AppendMessage<TMessage>(TMessage message, string? idempotencyKey = null, string? sender = null, string? receiver = null) where TMessage : class
        => await _messagesSender.AppendMessage(_storedId, message, idempotencyKey, sender, receiver);
}

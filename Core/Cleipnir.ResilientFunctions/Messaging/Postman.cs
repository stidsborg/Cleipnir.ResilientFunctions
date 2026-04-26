using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class Postman(MessageWriters messageWriters)
{
    public Task SendMessage<TMessage>(
        StoredId instance,
        TMessage message,
        string? idempotencyKey = null
    ) where TMessage : class => messageWriters.For(instance).AppendMessage(message, idempotencyKey);

    public async Task SendMessages(IReadOnlyList<BatchedMessage> messages)
        => await messageWriters.AppendMessages(messages);
}
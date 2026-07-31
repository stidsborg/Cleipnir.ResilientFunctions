using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageWriters
{
    private readonly StoredType _storedType;
    private readonly MessagesSender _messagesSender;

    internal MessageWriters(StoredType storedType, MessagesSender messagesSender)
    {
        _storedType = storedType;
        _messagesSender = messagesSender;
    }

    public MessageWriter For(FlowInstance instance) => For(StoredId.Create(_storedType, instance.Value));

    internal MessageWriter For(StoredId storedId) => new(storedId, _messagesSender);

    public async Task AppendMessages(IReadOnlyList<BatchedMessage> messages)
        => await _messagesSender.AppendMessages(
            messages
                .Select(m => _messagesSender.Serialize(StoredId.Create(_storedType, m.Instance.Value), m.Message, m.IdempotencyKey))
                .ToList()
        );
}

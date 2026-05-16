using System;
using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Domain;

public record InitialState(
    IEnumerable<MessageAndIdempotencyKey> Messages,
    IEnumerable<InitialEffect> Effects
)
{
    public static InitialState CreateWithMessagesOnly(IEnumerable<MessageAndIdempotencyKey> messages) 
        => new(messages, Effects: []);
}

public record InitialEffect(EffectId Id, object? Value = null, Exception? Exception = null, WorkStatus? Status = null, string? Alias = null);
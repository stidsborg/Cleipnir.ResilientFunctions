using System;
using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Domain;

public record InitialState(
    IEnumerable<MessageAndIdempotencyKey> Messages,
    IEnumerable<InitialEffect> Effects
);

public record InitialEffect(string Id, object? Value = null, Exception? Exception = null, WorkStatus? Status = null);
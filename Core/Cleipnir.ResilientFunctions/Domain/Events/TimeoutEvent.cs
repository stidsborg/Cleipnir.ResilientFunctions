using System;

namespace Cleipnir.ResilientFunctions.Domain.Events;

public record TimeoutEvent(EffectId TimeoutId, DateTime Expiration);
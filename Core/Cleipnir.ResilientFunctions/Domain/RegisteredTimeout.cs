using System;

namespace Cleipnir.ResilientFunctions.Domain;

public record RegisteredTimeout(EffectId TimeoutId, DateTime Expiry);
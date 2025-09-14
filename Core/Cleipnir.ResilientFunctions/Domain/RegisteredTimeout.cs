using System;
using Cleipnir.ResilientFunctions.CoreRuntime;

namespace Cleipnir.ResilientFunctions.Domain;

public record RegisteredTimeout(EffectId TimeoutId, DateTime Expiry, TimeoutStatus Status);
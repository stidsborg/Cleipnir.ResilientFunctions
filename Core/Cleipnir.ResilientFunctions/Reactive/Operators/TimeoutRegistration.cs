using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Reactive.Operators;

public record TimeoutRegistration(EffectId EffectId, DateTime ExpiresAt);
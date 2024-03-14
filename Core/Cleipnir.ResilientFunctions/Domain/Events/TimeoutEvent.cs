using System;

namespace Cleipnir.ResilientFunctions.Domain.Events;

public record TimeoutEvent(string TimeoutId, DateTime Expiration);
public record TimeoutId(string Value);
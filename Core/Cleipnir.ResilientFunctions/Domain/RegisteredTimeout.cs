using System;

namespace Cleipnir.ResilientFunctions.Domain;

public record RegisteredTimeout(TimeoutId TimeoutId, DateTime Expiry);
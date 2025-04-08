using System;

namespace Cleipnir.ResilientFunctions.Reactive.Operators;

public record RegisterTimeoutResult(DateTime? TimeoutExpiry, bool AppendedTimeoutToMessages);
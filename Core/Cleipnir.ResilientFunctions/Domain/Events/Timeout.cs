using System;

namespace Cleipnir.ResilientFunctions.Domain.Events;

public record Timeout(string TimeoutId, DateTime Occured);
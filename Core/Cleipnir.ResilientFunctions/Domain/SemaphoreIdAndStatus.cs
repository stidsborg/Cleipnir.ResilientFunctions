namespace Cleipnir.ResilientFunctions.Domain;

public record SemaphoreIdAndStatus(string Group, string Instance, SemaphoreStatus Status);
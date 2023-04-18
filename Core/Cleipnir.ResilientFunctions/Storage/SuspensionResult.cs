namespace Cleipnir.ResilientFunctions.Storage;

public enum SuspensionResult
{
    Success,
    ConcurrentStateModification,
    EventCountMismatch
}
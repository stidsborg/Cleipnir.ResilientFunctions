namespace Cleipnir.ResilientFunctions.Domain;

public enum SemaphoreStatus
{
    Created = 0,
    Waiting = 1,
    Acquired = 2,
    Released = 3
}
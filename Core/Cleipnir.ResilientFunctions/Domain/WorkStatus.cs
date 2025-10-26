namespace Cleipnir.ResilientFunctions.Domain;

public enum WorkStatus : byte
{
    NotStarted = 0,
    Started = 1,
    Completed = 2,
    Failed = 3,
}
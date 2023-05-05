namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public enum PersistResultReturn
{
    Success = 0,
    ScheduleReInvocation = 1,
    ConcurrentModification = 2,
}
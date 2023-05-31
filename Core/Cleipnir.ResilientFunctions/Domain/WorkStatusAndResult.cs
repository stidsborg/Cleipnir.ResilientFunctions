namespace Cleipnir.ResilientFunctions.Domain;

public struct WorkStatusAndResult<T>
{
    public WorkStatus Status { get; set; }
    public T Result { get; set; }
}
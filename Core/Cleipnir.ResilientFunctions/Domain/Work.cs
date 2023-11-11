namespace Cleipnir.ResilientFunctions.Domain;

public struct Work<T>
{
    public WorkStatus Status { get; set; }
    public T Result { get; set; }

    public void Deconstruct(out WorkStatus status, out T result)
    {
        status = Status;
        result = Result;
    }
}
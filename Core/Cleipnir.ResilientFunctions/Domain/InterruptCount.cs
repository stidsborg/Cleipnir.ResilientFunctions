namespace Cleipnir.ResilientFunctions.Domain;

public struct InterruptCount
{
    public long Value { get; }
    
    public InterruptCount(long value)
    {
        Value = value;
    }
}
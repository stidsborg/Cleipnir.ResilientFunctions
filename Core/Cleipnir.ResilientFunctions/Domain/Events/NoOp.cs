namespace Cleipnir.ResilientFunctions.Domain.Events;

public record NoOp
{
    public static NoOp Instance { get; } = new();
}
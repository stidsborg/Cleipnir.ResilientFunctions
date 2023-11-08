using System;

namespace Cleipnir.ResilientFunctions.Reactive.Origin;

internal struct EmittedEvent
{
    public object? Event { get; }
    public bool Completion { get; }
    public Exception? EmittedException { get; }

    public EmittedEvent(object? @event, bool completion, Exception? emittedException)
    {
        Event = @event;
        Completion = completion;
        EmittedException = emittedException;
    }
}
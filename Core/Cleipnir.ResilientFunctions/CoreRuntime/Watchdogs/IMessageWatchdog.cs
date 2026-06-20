using System.Collections.Generic;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

/// <summary>
/// The slice of <see cref="MessageWatchdog"/> a QueueManager depends on: reporting message positions it has
/// deleted from the store so the watchdog can drop them from its ignore-set. Exists so tests that hand-roll a
/// QueueManager can pass a no-op stub instead of a fully wired watchdog.
/// </summary>
internal interface IMessageWatchdog
{
    void RemoveMessages(IReadOnlyList<long> positions);
}

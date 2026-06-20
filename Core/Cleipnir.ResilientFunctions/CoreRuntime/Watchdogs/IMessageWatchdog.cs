using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

/// <summary>
/// The slice of <see cref="MessageWatchdog"/> a QueueManager depends on: deleting handled messages from the
/// store and dropping their positions from the watchdog's ignore-set. Exists so tests that hand-roll a
/// QueueManager can pass a no-op stub instead of a fully wired watchdog.
/// </summary>
internal interface IMessageWatchdog
{
    Task RemoveMessages(StoredId storedId, IReadOnlyList<long> positions);
}

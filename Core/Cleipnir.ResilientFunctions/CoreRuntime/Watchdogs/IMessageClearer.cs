using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

/// <summary>
/// The slice of <see cref="MessageClearer"/> a QueueManager depends on: deleting handled messages from the store
/// and dropping their positions from the watchdog's ignore-set. Exists so tests that hand-roll a QueueManager can
/// pass a no-op stub instead of a fully wired clearer.
/// </summary>
internal interface IMessageClearer
{
    Task Clear(IReadOnlyList<long> positions);
}

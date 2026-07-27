using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

/// <summary>
/// The slice of <see cref="MessageClearer"/> its collaborators depend on: deleting terminally handled messages
/// from the store (and dropping their positions from the watchdog's ignore-set) via <see cref="Clear"/>.
/// Reopening positions is not part of the slice - it is the MessageWatchdog's own bookkeeping on the concrete
/// clearer. Exists so tests can pass a no-op stub instead of a fully wired clearer.
/// </summary>
internal interface IMessageClearer
{
    Task Clear(IReadOnlyList<long> positions);
}

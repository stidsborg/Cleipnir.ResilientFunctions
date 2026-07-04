using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

/// <summary>
/// The slice of <see cref="MessageClearer"/> its collaborators depend on: the QueueManager deletes handled messages
/// from the store (and drops their positions from the watchdog's ignore-set) via <see cref="Clear"/>, while the
/// FlowsManager drops the positions of flows it could not restart back out of the ignore-set - without deleting them
/// from the store - via <see cref="ReopenPositions"/>. Exists so tests can pass a no-op stub instead of a fully
/// wired clearer.
/// </summary>
internal interface IMessageClearer
{
    Task Clear(IReadOnlyList<long> positions);
    void ReopenPositions(IEnumerable<long> positions);
}

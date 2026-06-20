using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

/// <summary>
/// Fetches the not-yet-skipped messages for a single flow. Implemented by the MessageWatchdog so that all
/// IMessageStore access is owned by the watchdog: the QueueManager pulls on-demand through this delegate
/// instead of reaching into the message store itself.
/// </summary>
public delegate Task<IReadOnlyList<StoredMessage>> MessageFetcher(StoredId storedId, IReadOnlyList<long> skipPositions);

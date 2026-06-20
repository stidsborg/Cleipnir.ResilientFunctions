using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class MessageWatchdog(
    IMessageStore messageStore,
    FlowsManagers flowsManagers,
    MessageClearer messageClearer,
    ClusterInfo clusterInfo,
    ShutdownCoordinator shutdownCoordinator,
    UnhandledExceptionHandler unhandledExceptionHandler,
    TimeSpan checkFrequency,
    TimeSpan delayStartUp,
    UtcNow utcNow)
{
    /// <summary>
    /// On-demand fetch for a single flow, used by its <see cref="Queuing.QueueManager"/> instead of reaching into
    /// the message store directly - so all <see cref="IMessageStore"/> access stays owned by the watchdog. Unlike
    /// the poll loop below this does not consult the clearer's pushed-set: the caller passes its own already-fetched
    /// positions as <paramref name="skipPositions"/>, which keeps the subscribe-time and restart-from-replay fetch
    /// behaviour identical to the previous in-QueueManager fetch.
    /// </summary>
    public Task<IReadOnlyList<StoredMessage>> FetchMessages(StoredId storedId, IReadOnlyList<long> skipPositions)
        => messageStore.GetMessages(storedId, skipPositions);

    public async Task Start()
    {
        await Task.Delay(delayStartUp);

        Start:
        try
        {
            while (!shutdownCoordinator.ShutdownInitiated)
            {
                var now = utcNow();

                // Messages destined for flows currently owned by this replica (replica = COALESCE(owner, publisher)).
                // The clearer's ignore-set excludes messages already pushed (and not yet cleared) so they are not
                // re-delivered. FlowsManagers.Push routes each group to its flow type's manager and delivers only
                // to live flows; entries for non-live flows (or unregistered types) are ignored.
                var nonClearedPositions = messageClearer.NonClearedPositions();

                var messageGroups = await messageStore.GetMessagesForReplica(clusterInfo.ReplicaId, nonClearedPositions);
                if (messageGroups.Count > 0)
                {
                    messageClearer.MarkPushed(messageGroups.SelectMany(group => group.Messages).Select(message => message.Position));
                    await flowsManagers.Push(messageGroups);
                }

                var timeElapsed = utcNow() - now;
                var delay = (checkFrequency - timeElapsed).RoundUpToZero();

                await Task.Delay(delay);
            }
        }
        catch (Exception thrownException)
        {
            unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    $"{nameof(MessageWatchdog)} execution failed - retrying in 5 seconds",
                    innerException: thrownException
                )
            );

            await Task.Delay(5_000);
            goto Start;
        }
    }
}

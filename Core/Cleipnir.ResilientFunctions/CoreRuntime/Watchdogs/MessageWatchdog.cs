using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

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
    public async Task Start()
    {
        await Task.Delay(delayStartUp);

        Start:
        try
        {
            while (!shutdownCoordinator.ShutdownInitiated)
            {
                var now = utcNow();

                await PushOnce();

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

    /// <summary>
    /// One fetch-and-push cycle: fetches this replica's not-yet-pushed messages (replica = COALESCE(owner, publisher)),
    /// marks them pushed so the next poll skips them, and routes each group to its flow type's manager, which delivers
    /// only to live flows; entries for non-live flows (or unregistered types) are ignored. Run on the poll loop, and
    /// on-demand at flow initialization so a freshly started/resumed flow receives its pending messages immediately
    /// instead of waiting for the next poll.
    /// </summary>
    public async Task PushOnce()
    {
        var nonClearedPositions = messageClearer.NonClearedPositions();

        var messageGroups = await messageStore.GetMessagesForReplica(clusterInfo.ReplicaId, nonClearedPositions);
        if (messageGroups.Count > 0)
        {
            messageClearer.MarkPushed(messageGroups.SelectMany(group => group.Messages).Select(message => message.Position));
            await flowsManagers.Push(messageGroups);
        }
    }
}

using System;
using System.Linq;
using System.Threading;
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
    private volatile TaskCompletionSource _wakeSignal = NewWakeSignal();

    private static TaskCompletionSource NewWakeSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Wakes the watchdog if it is sleeping between polls, so a just-appended message is delivered immediately
    /// instead of waiting out the remainder of the poll interval. Context-free and cheap: it only completes a
    /// signal - the fetch-and-push itself always runs on the watchdog's own loop.
    /// </summary>
    public void Notify() => _wakeSignal.TrySetResult();

    public async Task Start()
    {
        await Task.Delay(delayStartUp);

        Start:
        try
        {
            while (!shutdownCoordinator.ShutdownInitiated)
            {
                var now = utcNow();

                // Re-arm before fetching: a Notify arriving while the push runs completes the new signal, making
                // the wait below return immediately - so no wake-up is ever lost.
                var wakeSignal = _wakeSignal = NewWakeSignal();

                await PushOnce();

                var timeElapsed = utcNow() - now;
                var delay = (checkFrequency - timeElapsed).RoundUpToZero();

                await Task.WhenAny(wakeSignal.Task, Task.Delay(delay));
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
    /// marks them pushed so the next poll skips them, and routes each group to its flow type's manager - delivering
    /// to live flows and claiming/restarting the rest.
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

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class MessageWatchdog(
    IMessageStore messageStore,
    FlowsManagers flowsManagers,
    MessageDeserializer messageDeserializer,
    DlqManager dlqManager,
    MessageClearer messageClearer,
    ClusterInfo clusterInfo,
    ShutdownCoordinator shutdownCoordinator,
    UnhandledExceptionHandler unhandledExceptionHandler,
    TimeSpan checkFrequency,
    UtcNow utcNow)
{
    private volatile TaskCompletionSource _wakeSignal = NewWakeSignal();

    // Flows the PostponedWatchdog has asked to be restarted by the next fetch-and-push cycle (RequestRestarts).
    private readonly Lock _restartRequestsLock = new();
    private HashSet<StoredId> _restartRequests = new();

    private static TaskCompletionSource NewWakeSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Wakes the watchdog if it is sleeping between polls, so a just-appended message is delivered immediately
    /// instead of waiting out the remainder of the poll interval. Context-free and cheap: it only completes a
    /// signal - the fetch-and-push itself always runs on the watchdog's own loop.
    /// </summary>
    public void Notify() => _wakeSignal.TrySetResult();

    /// <summary>
    /// Requests the given flows be restarted by the next fetch-and-push cycle - the PostponedWatchdog's
    /// hand-over. Routing expired-flow restarts through the message path pairs every restart with a fetch of
    /// this replica's pending messages, so a restarted flow receives its rows in-hand (reconciled at queue
    /// initialization) instead of racing them as live-pushes. A request is consumed by its cycle whatever the
    /// outcome - a flow that could not be claimed, or a cycle that failed outright, is simply re-detected as
    /// expired by the PostponedWatchdog's next poll.
    /// </summary>
    public void RequestRestarts(IReadOnlyList<StoredId> storedIds)
    {
        if (storedIds.Count == 0)
            return;

        lock (_restartRequestsLock)
            foreach (var storedId in storedIds)
                _restartRequests.Add(storedId);

        Notify();
    }

    private IReadOnlyCollection<StoredId> DrainRestartRequests()
    {
        lock (_restartRequestsLock)
        {
            if (_restartRequests.Count == 0)
                return [];

            var drained = _restartRequests;
            _restartRequests = new HashSet<StoredId>();
            return drained;
        }
    }

    public async Task Start()
    {
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
    /// marks them pushed so the next poll skips them, deserializes them into the object-form pipeline and pushes
    /// them - flat, each message paired with its target flow - to the flow-type managers, which deliver to live
    /// flows and claim/restart the rest. This is the fetch boundary: messages failing deserialization are dead
    /// lettered by the deserializer and drop out of the batch, empty restart-pokes carry no payload and travel
    /// as bare store positions, and messages for flow types not registered on this replica have no manager to
    /// deliver to - they stay byte-form and are held by the DlqManager for the grace period, then dead lettered.
    /// Pending restart requests ride the cycle as synthetic row-less pokes, restarting their flows with the
    /// batch's rows in hand.
    /// </summary>
    public async Task PushOnce()
    {
        // Drained before the fetch: a request exists only once its flow is claimable, which is only after the
        // flow's message rows became fetchable by this replica (the ReplicaWatchdog completes message takeover
        // before making a crashed replica's flows claimable) - so the batch paired with the request below
        // contains every pending row of the requested flow, and restart and rows always travel together.
        var restartRequests = DrainRestartRequests();

        var nonClearedPositions = messageClearer.NonClearedPositions();

        var messages = await messageStore.GetMessagesForReplica(clusterInfo.ReplicaId, nonClearedPositions);
        if (messages.Count == 0 && restartRequests.Count == 0)
            return;

        messageClearer.MarkPushed(messages.Select(message => message.Position));

        var unregistered = ImmutableList<StoredMessage>.Empty;
        var incoming = new List<IncomingMessage>(messages.Count + restartRequests.Count);
        foreach (var storedMessage in messages)
        {
            if (!flowsManagers.IsRegistered(storedMessage.StoredId.Type))
            {
                unregistered = unregistered.Add(storedMessage);
                continue;
            }

            if (storedMessage.IsEmpty)
            {
                incoming.Add(IncomingMessage.CreateEmpty(storedMessage.StoredId, storedMessage.Position));
                continue;
            }

            var incomingMessage = await messageDeserializer.DeserializeOrDeadLetter(storedMessage);
            if (incomingMessage is not null)
                incoming.Add(incomingMessage);
        }

        if (unregistered.Count > 0)
            dlqManager.MoveToDlqAfterGracePeriod(unregistered);

        // The restart requests join the same dispatch as the fetched messages, as synthetic row-less pokes
        // (always for registered types - the PostponedWatchdog only requests types registered with it): grouped
        // with their flow's fetched rows they restart the flow with those rows in hand, while a request whose
        // flow is already live dissolves into a no-op in the queue manager.
        foreach (var storedId in restartRequests)
            incoming.Add(IncomingMessage.CreateSyntheticPoke(storedId));

        if (incoming.Count > 0)
            await flowsManagers.Push(incoming);
    }
}

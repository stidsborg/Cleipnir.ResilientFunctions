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

    private static TaskCompletionSource NewWakeSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Wakes the watchdog if it is sleeping between polls, so a just-appended message is delivered immediately
    /// instead of waiting out the remainder of the poll interval. Context-free and cheap: it only completes a
    /// signal - the fetch-and-push itself always runs on the watchdog's own loop.
    /// </summary>
    public void Notify() => _wakeSignal.TrySetResult();

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
    /// </summary>
    public async Task PushOnce()
    {
        var nonClearedPositions = messageClearer.NonClearedPositions();

        var messages = await messageStore.GetMessagesForReplica(clusterInfo.ReplicaId, nonClearedPositions);
        if (messages.Count == 0)
            return;

        messageClearer.MarkPushed(messages.Select(message => message.Position));

        var unregistered = ImmutableList<StoredMessage>.Empty;
        var incoming = new List<IncomingMessage>(messages.Count);
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

        if (incoming.Count > 0)
            await flowsManagers.Push(incoming);
    }
}

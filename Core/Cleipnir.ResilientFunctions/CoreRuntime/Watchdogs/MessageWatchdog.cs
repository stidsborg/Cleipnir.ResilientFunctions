using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class MessageWatchdog
{
    private readonly IMessageStore _messageStore;
    private readonly FlowsManagers _flowsManagers;
    private readonly ClusterInfo _clusterInfo;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    private readonly UtcNow _utcNow;

    // Positions already pushed to this replica's flows. Passed as ignore-set so messages are not re-delivered
    // on subsequent ticks. A QueueManager calls RemoveMessages once it has deleted the corresponding messages
    // from the store, trimming this set so it does not grow without bound. Guarded by _pushedPositionsLock
    // because RemoveMessages runs on flow threads concurrently with the watchdog loop.
    private readonly HashSet<long> _pushedPositions = new();
    private readonly Lock _pushedPositionsLock = new();

    public MessageWatchdog(
        IMessageStore messageStore,
        FlowsManagers flowsManagers,
        ClusterInfo clusterInfo,
        ShutdownCoordinator shutdownCoordinator,
        UnhandledExceptionHandler unhandledExceptionHandler,
        TimeSpan checkFrequency,
        TimeSpan delayStartUp,
        UtcNow utcNow)
    {
        _messageStore = messageStore;
        _flowsManagers = flowsManagers;
        _clusterInfo = clusterInfo;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _checkFrequency = checkFrequency;
        _delayStartUp = delayStartUp;
        _utcNow = utcNow;
    }

    public async Task Start()
    {
        await Task.Delay(_delayStartUp);

        Start:
        try
        {
            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                var now = _utcNow();

                // Messages destined for flows currently owned by this replica (replica = COALESCE(owner, publisher)).
                // FlowsManagers.Push routes each group to its flow type's manager and delivers only to live
                // flows; entries for non-live flows (or unregistered types) are ignored.
                List<long> ignorePositions;
                lock (_pushedPositionsLock)
                    ignorePositions = _pushedPositions.ToList();

                var messageGroups = await _messageStore.GetMessagesForReplica(_clusterInfo.ReplicaId, ignorePositions);
                if (messageGroups.Count > 0)
                {
                    lock (_pushedPositionsLock)
                        foreach (var group in messageGroups)
                            foreach (var message in group.Messages)
                                _pushedPositions.Add(message.Position);

                    await _flowsManagers.Push(messageGroups);
                }

                var timeElapsed = _utcNow() - now;
                var delay = (_checkFrequency - timeElapsed).RoundUpToZero();

                await Task.Delay(delay);
            }
        }
        catch (Exception thrownException)
        {
            _unhandledExceptionHandler.Invoke(
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
    /// Called by a QueueManager once it has deleted the given message positions from the store, so the
    /// watchdog drops them from its ignore-set instead of carrying them forever.
    /// </summary>
    public void RemoveMessages(IReadOnlyList<long> positions)
    {
        if (positions.Count == 0)
            return;

        lock (_pushedPositionsLock)
            foreach (var position in positions)
                _pushedPositions.Remove(position);
    }
}

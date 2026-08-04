using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

/// <summary>
/// Detects expired (postponed/suspended-due and rescheduled-after-crash) flows and appends an empty restart-poke
/// for each (<see cref="MessageSender.SendRestartPokes"/>) rather than claiming them itself. Every restart thereby
/// goes through the message path: the poke is fetched by this replica's MessageWatchdog together with any pending
/// message rows of the flow, so the restart receives the rows in-hand at queue initialization - a message-blind
/// restart racing its flow's rows cannot happen. This watchdog therefore only detects: a poke whose restart does
/// not come to pass is retried by the poke's own store residency (reopened/re-fetched), and a flow still expired
/// on a later poll is simply poked again - duplicate pokes are consumed together by the restart they trigger.
/// </summary>
internal class PostponedWatchdog
{
    private readonly IFunctionStore _functionStore;
    // Lazy: the sender is constructed after the watchdogs (it notifies the MessageWatchdog); resolved on first use.
    private readonly Func<MessageSender> _messageSender;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    private readonly TimeSpan _checkFrequency;
    private readonly ClusterInfo _clusterInfo;

    private volatile ImmutableHashSet<StoredType> _registeredTypes = ImmutableHashSet<StoredType>.Empty;

    private readonly UtcNow _utcNow;

    public PostponedWatchdog(
        IFunctionStore functionStore,
        Func<MessageSender> messageSender,
        ShutdownCoordinator shutdownCoordinator, UnhandledExceptionHandler unhandledExceptionHandler,
        TimeSpan checkFrequency,
        ClusterInfo clusterInfo,
        UtcNow utcNow)
    {
        _functionStore = functionStore;
        _messageSender = messageSender;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _checkFrequency = checkFrequency;
        _clusterInfo = clusterInfo;
        _utcNow = utcNow;
    }

    public void Register(StoredType storedType)
        => _registeredTypes = _registeredTypes.Add(storedType);

    /// <summary>
    /// Started by the FunctionsRegistry once - never at registration time: the loop shards by cluster offset and
    /// pokes flows for this replica, both of which require the replica to have joined the cluster first.
    /// </summary>
    public void Start() => Task.Run(Run);

    private async Task Run()
    {
        Start:
        try
        {
            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                var now = _utcNow();

                var eligibleFunctions = await _functionStore.GetExpiredFunctions(expiresBefore: now.Ticks);
                var registeredTypes = _registeredTypes;
                var ownedFunctions = eligibleFunctions
                    .Where(id => registeredTypes.Contains(id.Type))
                    .Where(_clusterInfo.OwnedByThisReplica)
                    .ToList();

                if (ownedFunctions.Count > 0)
                    await _messageSender().SendRestartPokes(ownedFunctions);

                var timeElapsed = _utcNow() - now;
                var delay = (_checkFrequency - timeElapsed).RoundUpToZero();

                await Task.Delay(delay);
            }
        }
        catch (Exception thrownException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    $"{nameof(PostponedWatchdog)} execution failed - retrying in 5 seconds",
                    innerException: thrownException
                )
            );

            await Task.Delay(5_000);
            goto Start;
        }
    }
}
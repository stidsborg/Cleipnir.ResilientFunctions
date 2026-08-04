using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

/// <summary>
/// Detects expired (postponed/suspended-due and rescheduled-after-crash) flows and hands them to the
/// <see cref="MessageWatchdog"/> for restart (<see cref="MessageWatchdog.RequestRestarts"/>) rather than
/// claiming them itself. Routing every restart through the message path pairs it with a fetch of the flow's
/// pending message rows, which then arrive in-hand at queue initialization - a message-blind restart racing its
/// flow's rows cannot happen. This watchdog therefore only detects: a hand-over whose restart does not come to
/// pass (claimed elsewhere, cycle failure) is simply re-detected on a later poll.
/// </summary>
internal class PostponedWatchdog
{
    private readonly IFunctionStore _functionStore;
    private readonly MessageWatchdog _messageWatchdog;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    private readonly TimeSpan _checkFrequency;
    private readonly ClusterInfo _clusterInfo;

    private volatile ImmutableHashSet<StoredType> _registeredTypes = ImmutableHashSet<StoredType>.Empty;

    private readonly UtcNow _utcNow;

    public PostponedWatchdog(
        IFunctionStore functionStore,
        MessageWatchdog messageWatchdog,
        ShutdownCoordinator shutdownCoordinator, UnhandledExceptionHandler unhandledExceptionHandler,
        TimeSpan checkFrequency,
        ClusterInfo clusterInfo,
        UtcNow utcNow)
    {
        _functionStore = functionStore;
        _messageWatchdog = messageWatchdog;
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
    /// restarts flows for this replica, both of which require the replica to have joined the cluster first.
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
                    _messageWatchdog.RequestRestarts(ownedFunctions);

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
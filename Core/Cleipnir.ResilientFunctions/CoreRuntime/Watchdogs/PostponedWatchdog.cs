using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class PostponedWatchdog
{
    private readonly IFunctionStore _functionStore;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    private readonly TimeSpan _checkFrequency;
    private readonly ClusterInfo _clusterInfo;
    
    private volatile ImmutableDictionary<StoredType, Tuple<ScheduleRestartFromWatchdog, AsyncSemaphore>> _flowsDictionary
        = ImmutableDictionary<StoredType, Tuple<ScheduleRestartFromWatchdog, AsyncSemaphore>>.Empty;

    private readonly UtcNow _utcNow;

    public PostponedWatchdog(
        IFunctionStore functionStore,
        ShutdownCoordinator shutdownCoordinator, UnhandledExceptionHandler unhandledExceptionHandler, 
        TimeSpan checkFrequency,
        ClusterInfo clusterInfo,
        UtcNow utcNow)
    {
        _functionStore = functionStore;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _checkFrequency = checkFrequency;
        _clusterInfo = clusterInfo;
        _utcNow = utcNow;
    }

    public void Register(
        StoredType storedType,
        ScheduleRestartFromWatchdog scheduleRestart,
        AsyncSemaphore asyncSemaphore)
    {
        _flowsDictionary = _flowsDictionary.SetItem(storedType, Tuple.Create(scheduleRestart, asyncSemaphore));
    }

    /// <summary>
    /// Started by the FunctionsRegistry once - never at registration time: the loop shards by cluster offset and
    /// claims flows for this replica, both of which require the replica to have joined the cluster first.
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
                var flowsDictionary = _flowsDictionary;     
                var ownedFunctions = eligibleFunctions
                    .Where(id => flowsDictionary.ContainsKey(id.Type))
                    .Where(s => s.AsULong % _clusterInfo.ReplicaCount == _clusterInfo.Offset)
                    .ToList();
                
                var restarts = await _functionStore
                    .RestartExecutions(ownedFunctions, _clusterInfo.ReplicaId);

                foreach (var id in restarts.Keys)
                {
                    var (scheduleRestart, asyncSemaphore) = flowsDictionary[id.Type];
                    var (storedFlow, effects, session) = restarts[id];

                    var takenLock = await asyncSemaphore.Take();
                    try
                    {
                        // The restart hands over no messages - message fetching is the MessageWatchdog's sole
                        // responsibility; any pending messages are pushed to the restarted flow by its poll.
                        await scheduleRestart(
                            id,
                            new RestartedFunction(storedFlow, effects, StoredMessages: [], session),
                            onCompletion: () =>
                            {
                                takenLock.Dispose();
                            }
                        );
                    }
                    catch
                    {
                        takenLock.Dispose();
                        throw;
                    }
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
                    $"{nameof(PostponedWatchdog)} execution failed - retrying in 5 seconds",
                    innerException: thrownException
                )
            );
            
            await Task.Delay(5_000);
            goto Start;
        }
    }
}
using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class MessageWatchdog
{
    private readonly IMessageStore _messageStore;
    private readonly FlowsManager _flowsManager;
    private readonly ClusterInfo _clusterInfo;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    private readonly UtcNow _utcNow;

    public MessageWatchdog(
        IMessageStore messageStore,
        FlowsManager flowsManager,
        ClusterInfo clusterInfo,
        ShutdownCoordinator shutdownCoordinator,
        UnhandledExceptionHandler unhandledExceptionHandler,
        TimeSpan checkFrequency,
        TimeSpan delayStartUp,
        UtcNow utcNow)
    {
        _messageStore = messageStore;
        _flowsManager = flowsManager;
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
                // FlowsManager.Push delivers only to live flows; entries for non-live flows are ignored.
                var messagesByFlow = await _messageStore.GetMessagesForReplica(_clusterInfo.ReplicaId);
                if (messagesByFlow.Count > 0)
                    await _flowsManager.Push(messagesByFlow);

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
}

using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class InterruptedWatchdog
{
    private readonly IFunctionStore _functionStore;
    private readonly FlowsManager _flowsManager;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    private readonly UtcNow _utcNow;

    public InterruptedWatchdog(
        IFunctionStore functionStore,
        FlowsManager flowsManager,
        ShutdownCoordinator shutdownCoordinator,
        UnhandledExceptionHandler unhandledExceptionHandler,
        TimeSpan checkFrequency,
        TimeSpan delayStartUp,
        UtcNow utcNow)
    {
        _functionStore = functionStore;
        _flowsManager = flowsManager;
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

                var interrupted = await _functionStore.GetInterruptedFunctions();
                var owned = _flowsManager.FilterOwned(interrupted);
                if (owned.Count > 0)
                    await _flowsManager.Interrupt(owned);

                var timeElapsed = _utcNow() - now;
                var delay = (_checkFrequency - timeElapsed).RoundUpToZero();

                await Task.Delay(delay);
            }
        }
        catch (Exception thrownException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    $"{nameof(InterruptedWatchdog)} execution failed - retrying in 5 seconds",
                    innerException: thrownException
                )
            );

            await Task.Delay(5_000);
            goto Start;
        }
    }
}

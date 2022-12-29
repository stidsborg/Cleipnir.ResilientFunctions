using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class SuspensionWatchdog
{
    private readonly IFunctionStore _functionStore;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly WatchDogReInvokeFunc _reInvoke;
    private readonly AsyncSemaphore _asyncSemaphore;
    private readonly FunctionTypeId _functionTypeId;
    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;

    public SuspensionWatchdog(
        FunctionTypeId functionTypeId,
        IFunctionStore functionStore,
        WatchDogReInvokeFunc reInvoke,
        AsyncSemaphore asyncSemaphore,
        TimeSpan checkFrequency,
        TimeSpan delayStartUp,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _shutdownCoordinator = shutdownCoordinator;
        _asyncSemaphore = asyncSemaphore;
        _functionStore = functionStore;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _reInvoke = reInvoke;
        _checkFrequency = checkFrequency;
        _delayStartUp = delayStartUp;
    }

    public async Task Start()
    {
        if (_checkFrequency == TimeSpan.Zero) return;
        await Task.Delay(_delayStartUp);
        
        try
        {
            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                var eligibleSuspendedFunctions = 
                    await _functionStore.GetEligibleSuspendedFunctions(_functionTypeId);

                foreach (var eligibleSuspendedFunction in eligibleSuspendedFunctions)
                    _ = ReInvokeSuspendedFunction(eligibleSuspendedFunction);
                
                await Task.Delay(_checkFrequency);
            }
        }
        catch (Exception innerException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _functionTypeId,
                    $"{nameof(SuspensionWatchdog)} for '{_functionTypeId}' failed",
                    innerException
                )
            );
        }
    }

    private async Task ReInvokeSuspendedFunction(StoredEligibleSuspendedFunction spf)
    {
        var functionId = new FunctionId(_functionTypeId, spf.InstanceId);
        
        if (_shutdownCoordinator.ShutdownInitiated) return;

        using var @lock = await _asyncSemaphore.Take();
        try
        {
            await _reInvoke(spf.InstanceId, expectedEpoch: spf.Epoch);
        }
        catch (ObjectDisposedException) { } //ignore when rfunctions has been disposed
        catch (UnexpectedFunctionState) { } //ignore when the functions state has changed since fetching it
        catch (FunctionInvocationPostponedException) { }
        catch (FunctionInvocationSuspendedException) { }
        catch (Exception innerException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _functionTypeId,
                    $"{nameof(SuspensionWatchdog)} failed while executing: '{functionId}'",
                    innerException
                )
            );
        }
    }
}
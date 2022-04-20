using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Watchdogs;

internal class PostponedWatchdog
{
    private readonly IFunctionStore _functionStore;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly WatchDogReInvokeFunc _reInvoke;
    private readonly TimeSpan _checkFrequency;
    private readonly FunctionTypeId _functionTypeId;

    public PostponedWatchdog(
        FunctionTypeId functionTypeId,
        IFunctionStore functionStore,
        WatchDogReInvokeFunc reInvoke,
        TimeSpan checkFrequency,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _functionStore = functionStore;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
        _reInvoke = reInvoke;
        _checkFrequency = checkFrequency;
    }

    public async Task Start()
    {
        if (_checkFrequency == TimeSpan.Zero) return;

        try
        {
            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                await Task.Delay(_checkFrequency);
                if (_shutdownCoordinator.ShutdownInitiated) return;

                var now = DateTime.UtcNow;

                var expiresSoon = await _functionStore
                    .GetFunctionsWithStatus(
                        _functionTypeId,
                        Status.Postponed,
                        now.Add(_checkFrequency).Ticks
                    );

                foreach (var expireSoon in expiresSoon)
                    _ = SleepAndThenReInvoke(expireSoon, now);
            }
        }
        catch (Exception innerException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _functionTypeId,
                    $"{nameof(PostponedWatchdog)} failed while executing: '{_functionTypeId}'",
                    innerException
                )
            );
        }
    }

    private async Task SleepAndThenReInvoke(StoredFunctionStatus sfs, DateTime now)
    {
        var functionId = new FunctionId(_functionTypeId, sfs.InstanceId);
        if (_shutdownCoordinator.ShutdownInitiated) return;

        var postponedUntil = new DateTime(sfs.PostponedUntil!.Value, DateTimeKind.Utc);
        var delay = TimeSpanHelper.Max(postponedUntil - now, TimeSpan.Zero);
        await Task.Delay(delay);

        if (_shutdownCoordinator.ShutdownInitiated) return;

        try
        {
            using var _ = _shutdownCoordinator.RegisterRunningRFunc();
            var success = await _functionStore.TryToBecomeLeader(
                functionId,
                Status.Executing,
                expectedEpoch: sfs.Epoch,
                newEpoch: sfs.Epoch + 1
            );
            if (!success) return;
            
            await _reInvoke(
                sfs.InstanceId,
                expectedStatuses: new[] {Status.Executing},
                expectedEpoch: sfs.Epoch + 1
            );
        }
        catch (ObjectDisposedException) {} //ignore when rfunctions has been disposed
        catch (UnexpectedFunctionState) {} //ignore when the functions state has changed since fetching it
        catch (Exception innerException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _functionTypeId,
                    $"{nameof(PostponedWatchdog)} failed while executing: '{functionId}'",
                    innerException
                )
            );
        }
    }
}
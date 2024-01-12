using System;
using System.Collections.Generic;
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
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly ReInvoke _reInvoke;
    private readonly AsyncSemaphore _maxParallelismSemaphore;
    private readonly TimeSpan _postponedCheckFrequency;
    private readonly TimeSpan _delayStartUp;
    private readonly FunctionTypeId _functionTypeId;
    
    private readonly HashSet<FunctionInstanceId> _toBeExecuted = new();
    private readonly object _sync = new();

    public PostponedWatchdog(
        FunctionTypeId functionTypeId,
        IFunctionStore functionStore,
        ReInvoke reInvoke,
        AsyncSemaphore maxParallelismSemaphore,
        TimeSpan postponedCheckFrequency,
        TimeSpan delayStartUp,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _functionStore = functionStore;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
        _reInvoke = reInvoke;
        _maxParallelismSemaphore = maxParallelismSemaphore;
        _postponedCheckFrequency = postponedCheckFrequency;
        _delayStartUp = delayStartUp;
    }

    public async Task Start()
    {
        if (_postponedCheckFrequency == TimeSpan.Zero) return;
        await Task.Delay(_delayStartUp);
        
        try
        {
            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                var now = DateTime.UtcNow;

                var expiresSoon = await _functionStore
                    .GetPostponedFunctions(_functionTypeId, now.Add(_postponedCheckFrequency).Ticks);

                foreach (var expireSoon in expiresSoon)
                    _ = SleepAndThenReInvoke(expireSoon, now);
                
                await Task.Delay(_postponedCheckFrequency);
            }
        }
        catch (Exception innerException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _functionTypeId,
                    $"{nameof(PostponedWatchdog)} for '{_functionTypeId}' failed",
                    innerException
                )
            );
        }
    }

    private async Task SleepAndThenReInvoke(StoredPostponedFunction spf, DateTime now)
    {
        lock (_sync)
            if (!_toBeExecuted.Add(spf.InstanceId))
                return;

        var functionId = new FunctionId(_functionTypeId, spf.InstanceId);

        var postponedUntil = new DateTime(spf.PostponedUntil, DateTimeKind.Utc);
        var delay = TimeSpanHelper.Max(postponedUntil - now, TimeSpan.Zero);
        await Task.Delay(delay);

        if (_shutdownCoordinator.ShutdownInitiated) return;

        using var @lock = await _maxParallelismSemaphore.Take();
        try
        {
            while (DateTime.UtcNow < postponedUntil) //clock resolution means that we might wake up early 
                await Task.Yield();

            using var _ = _shutdownCoordinator.RegisterRunningRFunc();
            await _reInvoke(spf.InstanceId.Value, expectedEpoch: spf.Epoch);
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
                    $"{nameof(PostponedWatchdog)} failed while executing: '{functionId}'",
                    innerException
                )
            );
        }
        finally
        {
            lock (_sync)
                _toBeExecuted.Remove(spf.InstanceId);
        }
    }
}
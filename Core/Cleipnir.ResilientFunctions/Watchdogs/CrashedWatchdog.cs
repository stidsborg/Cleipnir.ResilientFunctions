using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Watchdogs;

internal class CrashedWatchdog
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly WatchDogReInvokeFunc _reInvoke;
    private readonly IFunctionStore _functionStore;
    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    
    private readonly AsyncSemaphore _asyncSemaphore;
    private readonly HashSet<string> _enqueued = new();
    private readonly object _sync = new();

    public CrashedWatchdog(
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
        _functionStore = functionStore;
        _reInvoke = reInvoke;
        _asyncSemaphore = asyncSemaphore;
        _checkFrequency = checkFrequency;
        _delayStartUp = delayStartUp;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async Task Start()
    {
        if (_checkFrequency == TimeSpan.Zero) return;
        await Task.Delay(_delayStartUp);
        
        try
        {
            var prevExecutingFunctions = new Dictionary<FunctionInstanceId, StoredExecutingFunction>();

            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                await Task.Delay(_checkFrequency);
                if (_shutdownCoordinator.ShutdownInitiated) return;

                var currExecutingFunctions = await _functionStore
                    .GetExecutingFunctions(_functionTypeId)
                    .SelectAsync(l =>
                        l.ToDictionary(
                            s => s.InstanceId,
                            s => s
                        )
                    );

                var hangingFunctions =
                    from prev in prevExecutingFunctions
                    join curr in currExecutingFunctions
                        on (prev.Key, prev.Value.Epoch, prev.Value.SignOfLife) 
                        equals (curr.Key, curr.Value.Epoch, curr.Value.SignOfLife)
                    select prev.Value;

                foreach (var sef in hangingFunctions.RandomlyPermutate())
                    _ = ReInvokeCrashedFunction(sef);

                prevExecutingFunctions = currExecutingFunctions;
            }
        }
        catch (Exception thrownException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _functionTypeId,
                    $"{nameof(CrashedWatchdog)} failed while executing: '{_functionTypeId}'",
                    innerException: thrownException
                )
            );
        }
    }

    private async Task ReInvokeCrashedFunction(StoredExecutingFunction sef)
    {
        lock (_sync)
            if (_enqueued.Contains(sef.InstanceId.Value))
                return;
            else
                _enqueued.Add(sef.InstanceId.Value);
        
        using var @lock = await _asyncSemaphore.Take();
        
        try
        {
            if (_shutdownCoordinator.ShutdownInitiated) return;

            await _reInvoke(
                sef.InstanceId,
                expectedStatuses: new[] { Status.Executing },
                expectedEpoch: sef.Epoch
            );
        }
        catch (ObjectDisposedException) { } //ignore when rfunctions has been disposed
        catch (UnexpectedFunctionState) { } //ignore when the functions state has changed since fetching it
        catch (FunctionInvocationPostponedException) { }
        catch (Exception innerException)
        {
            var functionId = new FunctionId(_functionTypeId, sef.InstanceId);
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _functionTypeId,
                    $"{nameof(CrashedWatchdog)} failed while executing: '{functionId}'",
                    innerException
                )
            );
        }
        finally
        {
            lock (_sync)
                _enqueued.Remove(sef.InstanceId.Value);
        }
    }
}
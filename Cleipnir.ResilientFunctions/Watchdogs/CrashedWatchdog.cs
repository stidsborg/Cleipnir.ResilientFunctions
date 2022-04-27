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
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;

    public CrashedWatchdog(
        FunctionTypeId functionTypeId,
        IFunctionStore functionStore,
        WatchDogReInvokeFunc reInvoke,
        TimeSpan checkFrequency,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _functionStore = functionStore;
        _reInvoke = reInvoke;
        _checkFrequency = checkFrequency;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async Task Start()
    {
        if (_checkFrequency == TimeSpan.Zero) return;
        try
        {
            var prevExecutingFunctions = new Dictionary<FunctionInstanceId, StoredExecutingFunction>();

            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                await Task.Delay(_checkFrequency);
                if (_shutdownCoordinator.ShutdownInitiated) return;

                var currExecutingFunctions = await _functionStore
                    .GetExecutingFunctions(_functionTypeId)
                    .TaskSelect(l =>
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

                foreach (var function in hangingFunctions.RandomlyPermutate())
                {
                    if (_shutdownCoordinator.ShutdownInitiated) return;

                    try
                    {
                        await _reInvoke(
                            function.InstanceId,
                            expectedStatuses: new[] { Status.Executing },
                            expectedEpoch: function.Epoch
                        );
                    }
                    catch (ObjectDisposedException) {} //ignore when rfunctions has been disposed
                    catch (UnexpectedFunctionState) {} //ignore when the functions state has changed since fetching it
                    catch (Exception innerException) 
                    {
                        var functionId = new FunctionId(_functionTypeId, function.InstanceId);
                        _unhandledExceptionHandler.Invoke(
                            new FrameworkException(
                                _functionTypeId,
                                $"{nameof(CrashedWatchdog)} failed while executing: '{functionId}'",
                                innerException
                            )
                        );
                    }
                }

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
}
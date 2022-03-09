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
using Cleipnir.ResilientFunctions.Watchdogs.Invocation;

namespace Cleipnir.ResilientFunctions.Watchdogs;

internal class CrashedWatchdog : IDisposable
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly WrappedInnerFunc _wrappedInnerFunc;

    private readonly IFunctionStore _functionStore;

    private readonly TimeSpan _checkFrequency;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly WrapperInnerFuncInvoker _wrapperInnerFuncInvoker;
        
    private volatile bool _disposed;
    private volatile bool _executing;

    public CrashedWatchdog(
        FunctionTypeId functionTypeId,
        WrappedInnerFunc wrappedInnerFunc,
        IFunctionStore functionStore,
        WrapperInnerFuncInvoker wrapperInnerFuncInvoker,
        TimeSpan checkFrequency,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _functionStore = functionStore;
        _wrappedInnerFunc = wrappedInnerFunc;
        _checkFrequency = checkFrequency;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _wrapperInnerFuncInvoker = wrapperInnerFuncInvoker;
        _disposed = !shutdownCoordinator.ObserveShutdown(DisposeAsync);
    }

    public async Task Start()
    {
        if (_checkFrequency == TimeSpan.Zero) return;
        try
        {
            var prevExecutingFunctions = new Dictionary<FunctionInstanceId, StoredFunctionStatus>();

            while (!_disposed)
            {
                _executing = false;
                await Task.Delay(_checkFrequency);
                if (_disposed) return;
                _executing = true;

                var currExecutingFunctions = await _functionStore
                    .GetFunctionsWithStatus(_functionTypeId, Status.Executing)
                    .TaskSelect(l =>
                        l.ToDictionary(
                            s => s.InstanceId,
                            s => s
                        )
                    );

                var hangingFunctions =
                    from prev in prevExecutingFunctions
                    join curr in currExecutingFunctions
                        on (prev.Key, prev.Value.SignOfLife) equals (curr.Key, curr.Value.SignOfLife)
                    select prev.Value;

                foreach (var function in hangingFunctions.RandomlyPermutate())
                {
                    if (_disposed) return;
                        
                    var functionId = new FunctionId(_functionTypeId, function.InstanceId);
                    var storedFunction = await _functionStore.GetFunction(functionId);
                    if (storedFunction?.Status != Status.Executing || function.Epoch != storedFunction.Epoch) continue;

                    try
                    {
                        await _wrapperInnerFuncInvoker.ReInvoke(functionId, storedFunction, _wrappedInnerFunc);
                    }
                    catch (Exception innerException)
                    {
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
        catch (Exception innerException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _functionTypeId,
                    $"{nameof(CrashedWatchdog)} failed while executing: '{_functionTypeId}'",
                    innerException
                )
            );
        }
        finally
        {
            _executing = false;
        }
    }

    private async Task DisposeAsync()
    {
        _disposed = true;
        await BusyWait.ForeverUntilAsync(() => !_executing);
    }

    public void Dispose() => DisposeAsync().Wait();
}
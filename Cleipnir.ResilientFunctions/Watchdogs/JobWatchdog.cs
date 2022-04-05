using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Watchdogs;

internal class JobWatchdog : IDisposable
{
    private readonly IFunctionStore _functionStore;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly TimeSpan _checkFrequency;
    private volatile bool _disposed;
    private volatile bool _executing;
    
    private readonly Dictionary<string, WatchDogReInvokeFunc> _reInvokeFuncs = new();
    private readonly object _sync = new();

    public JobWatchdog(
        IFunctionStore functionStore,
        TimeSpan checkFrequency,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionStore = functionStore;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _checkFrequency = checkFrequency;
        _disposed = !shutdownCoordinator.ObserveShutdown(DisposeAsync);
    }
    
    public void AddJob(string jobId, WatchDogReInvokeFunc reInvokeFunc)
    {
        lock (_sync)
        {
            var start = _reInvokeFuncs.Count == 0;
            _reInvokeFuncs[jobId] = reInvokeFunc;
            if (start)
                _ = Start();
        }
    }

    private async Task Start()
    {
        if (_checkFrequency == TimeSpan.Zero) return;

        try
        {
            while (!_disposed)
            {
                _executing = false;
                await Task.Delay(_checkFrequency);
                if (_disposed) return;
                _executing = true;

                var expires = await _functionStore
                    .GetFunctionsWithStatus("Job", Status.Postponed, DateTime.UtcNow.Ticks);

                foreach (var expired in expires)
                    _ = ReInvokeJob(expired.InstanceId.ToString(), expired.Epoch);
            }
        }
        catch (Exception innerException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    "Job",
                    $"{nameof(JobWatchdog)} failed while executing",
                    innerException
                )
            );
        }
        finally
        {
            _executing = false;
        }
    }

    private async Task ReInvokeJob(string jobId, int expectedEpoch)
    {
        if (_disposed) return;
        WatchDogReInvokeFunc? reInvoke;
        lock (_sync)
            _reInvokeFuncs.TryGetValue(jobId, out reInvoke);
                            
        if (reInvoke == null) return;
                    
        try
        {
            await reInvoke(
                jobId,
                expectedStatuses: new[] {Status.Postponed},
                expectedEpoch: expectedEpoch
            );
        }
        catch (UnexpectedFunctionState) {} //ignore when the functions state has changed since fetching it
        catch (Exception innerException)
        {
            var functionId = new FunctionId("Job", jobId);
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    "Job",
                    $"{nameof(JobWatchdog)} failed while executing: '{functionId}'",
                    innerException
                )
            );
        }
    }

    private Task DisposeAsync()
    {
        _disposed = true;
        return BusyWait.ForeverUntilAsync(() => !_executing);
    }

    public void Dispose() => DisposeAsync().Wait();
}
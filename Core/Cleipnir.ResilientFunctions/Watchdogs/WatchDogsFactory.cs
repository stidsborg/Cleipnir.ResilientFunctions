using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Watchdogs;

internal class WatchDogsFactory
{
    private readonly IFunctionStore _functionStore;
    private readonly TimeSpan _crashedCheckFrequency;
    private readonly TimeSpan _postponedCheckFrequency;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;

    public WatchDogsFactory(
        IFunctionStore functionStore,
        TimeSpan crashedCheckFrequency, 
        TimeSpan postponedCheckFrequency,
        UnhandledExceptionHandler unhandledExceptionHandler, 
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionStore = functionStore;
        _crashedCheckFrequency = crashedCheckFrequency;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
        _postponedCheckFrequency = postponedCheckFrequency;
    }

    public void CreateAndStart(FunctionTypeId functionTypeId, WatchDogReInvokeFunc reInvoke, int maxParallelInvocations)
    {
        var workQueue = new WorkQueue(maxParallelInvocations);
        var crashedWatchdog = new CrashedWatchdog(
            functionTypeId,
            _functionStore,
            reInvoke,
            workQueue,
            _crashedCheckFrequency,
            _unhandledExceptionHandler,
            _shutdownCoordinator
        );

        var postponedWatchdog = new PostponedWatchdog(
            functionTypeId,
            _functionStore,
            reInvoke,
            workQueue,
            _postponedCheckFrequency,
            _unhandledExceptionHandler,
            _shutdownCoordinator
        );

        _ = crashedWatchdog.Start();
        _ = postponedWatchdog.Start();
    }
}
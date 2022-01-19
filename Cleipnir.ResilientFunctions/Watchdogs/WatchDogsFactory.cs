using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Watchdogs.Invocation;

namespace Cleipnir.ResilientFunctions.Watchdogs;

internal class WatchDogsFactory
{
    private readonly IFunctionStore _functionStore;
    private readonly RFuncInvoker _rFuncInvoker;
    private readonly RActionInvoker _rActionInvoker;
    private readonly TimeSpan _crashedCheckFrequency;
    private readonly TimeSpan _postponedCheckFrequency;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;

    public WatchDogsFactory(
        IFunctionStore functionStore, 
        RFuncInvoker rFuncInvoker,
        RActionInvoker rActionInvoker,
        TimeSpan crashedCheckFrequency, 
        TimeSpan postponedCheckFrequency,
        UnhandledExceptionHandler unhandledExceptionHandler, 
        ShutdownCoordinator shutdownCoordinator
    )
    {
        _functionStore = functionStore;
        _rFuncInvoker = rFuncInvoker;
        _rActionInvoker = rActionInvoker;
        _crashedCheckFrequency = crashedCheckFrequency;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
        _postponedCheckFrequency = postponedCheckFrequency;
    }

    public void CreateAndStart<TReturn>(FunctionTypeId functionTypeId, RFunc<TReturn> rFunc) where TReturn : notnull 
    {
        var crashedWatchdog = new CrashedWatchdog<TReturn>(
            functionTypeId,
            rFunc,
            _functionStore,
            _rFuncInvoker,
            _crashedCheckFrequency,
            _unhandledExceptionHandler,
            _shutdownCoordinator
        );

        var postponedWatchdog = new PostponedWatchdog<TReturn>(
            functionTypeId,
            rFunc,
            _functionStore,
            _rFuncInvoker,
            _postponedCheckFrequency,
            _unhandledExceptionHandler,
            _shutdownCoordinator
        );

        _ = crashedWatchdog.Start();
        _ = postponedWatchdog.Start();

        Tuple.Create(crashedWatchdog, postponedWatchdog);
    } 
    
    public void CreateAndStart(FunctionTypeId functionTypeId, RAction rAction)  
    {
        var crashedWatchdog = new CrashedWatchdog(
            functionTypeId,
            rAction,
            _functionStore,
            _rActionInvoker,
            _crashedCheckFrequency,
            _unhandledExceptionHandler,
            _shutdownCoordinator
        );

        var postponedWatchdog = new PostponedWatchdog(
            functionTypeId,
            rAction,
            _functionStore,
            _rActionInvoker,
            _postponedCheckFrequency,
            _unhandledExceptionHandler,
            _shutdownCoordinator
        );

        _ = crashedWatchdog.Start();
        _ = postponedWatchdog.Start();

        Tuple.Create(crashedWatchdog, postponedWatchdog);
    } 
}
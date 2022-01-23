using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Watchdogs.Invocation;

namespace Cleipnir.ResilientFunctions.Watchdogs;

internal class WatchDogsFactory
{
    private readonly IFunctionStore _functionStore;
    private readonly ISignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly TimeSpan _crashedCheckFrequency;
    private readonly TimeSpan _postponedCheckFrequency;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;

    public WatchDogsFactory(
        IFunctionStore functionStore, 
        ISignOfLifeUpdaterFactory signOfLifeUpdaterFactory,
        TimeSpan crashedCheckFrequency, 
        TimeSpan postponedCheckFrequency,
        UnhandledExceptionHandler unhandledExceptionHandler, 
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionStore = functionStore;
        _crashedCheckFrequency = crashedCheckFrequency;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _postponedCheckFrequency = postponedCheckFrequency;
    }

    public void CreateAndStart<TReturn>(FunctionTypeId functionTypeId, ISerializer serializer, RFunc<TReturn> rFunc) where TReturn : notnull
    {
        var rFuncInvoker = new RFuncInvoker(
            _functionStore,
            serializer,
            _signOfLifeUpdaterFactory,
            _unhandledExceptionHandler,
            _shutdownCoordinator
        );
        
        var crashedWatchdog = new CrashedWatchdog<TReturn>(
            functionTypeId,
            rFunc,
            _functionStore,
            rFuncInvoker,
            _crashedCheckFrequency,
            _unhandledExceptionHandler,
            _shutdownCoordinator
        );

        var postponedWatchdog = new PostponedWatchdog<TReturn>(
            functionTypeId,
            rFunc,
            _functionStore,
            rFuncInvoker,
            _postponedCheckFrequency,
            _unhandledExceptionHandler,
            _shutdownCoordinator
        );

        _ = crashedWatchdog.Start();
        _ = postponedWatchdog.Start();
    } 
    
    public void CreateAndStart(FunctionTypeId functionTypeId, ISerializer serializer, RAction rAction)  
    {
        var rActionInvoker = new RActionInvoker(_functionStore, serializer, _signOfLifeUpdaterFactory, _unhandledExceptionHandler, _shutdownCoordinator);
        var crashedWatchdog = new CrashedWatchdog(
            functionTypeId,
            rAction,
            _functionStore,
            rActionInvoker,
            _crashedCheckFrequency,
            _unhandledExceptionHandler,
            _shutdownCoordinator
        );

        var postponedWatchdog = new PostponedWatchdog(
            functionTypeId,
            rAction,
            _functionStore,
            rActionInvoker,
            _postponedCheckFrequency,
            _unhandledExceptionHandler,
            _shutdownCoordinator
        );

        _ = crashedWatchdog.Start();
        _ = postponedWatchdog.Start();

        Tuple.Create(crashedWatchdog, postponedWatchdog);
    } 
}
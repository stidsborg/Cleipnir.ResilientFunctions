using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Watchdogs.Invocation;

namespace Cleipnir.ResilientFunctions.Watchdogs;

internal class WatchDogsFactory
{
    private readonly IFunctionStore _functionStore;
    private readonly RFuncInvoker _rFuncInvoker;
    private readonly TimeSpan _crashedCheckFrequency;
    private readonly TimeSpan _postponedCheckFrequency;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    public WatchDogsFactory(
        IFunctionStore functionStore, 
        RFuncInvoker rFuncInvoker, 
        TimeSpan crashedCheckFrequency, 
        TimeSpan postponedCheckFrequency,
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionStore = functionStore;
        _rFuncInvoker = rFuncInvoker;
        _crashedCheckFrequency = crashedCheckFrequency;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _postponedCheckFrequency = postponedCheckFrequency;
    }

    public Tuple<CrashedWatchdog<TReturn>, PostponedWatchdog<TReturn>> CreateAndStart<TReturn>(
        FunctionTypeId functionTypeId,
        RFunc<TReturn> rFunc
    ) where TReturn : notnull 
    {
        var crashedWatchdog = new CrashedWatchdog<TReturn>(
            functionTypeId,
            rFunc,
            _functionStore,
            _rFuncInvoker,
            _crashedCheckFrequency,
            _unhandledExceptionHandler
        );

        var postponedWatchdog = new PostponedWatchdog<TReturn>(
            functionTypeId,
            rFunc,
            _functionStore,
            _rFuncInvoker,
            _postponedCheckFrequency,
            _unhandledExceptionHandler
        );

        _ = crashedWatchdog.Start();
        _ = postponedWatchdog.Start();
        
        return Tuple.Create(crashedWatchdog, postponedWatchdog);
    } 
    
    public Tuple<CrashedWatchdog, PostponedWatchdog> CreateAndStart(
        FunctionTypeId functionTypeId,
        RAction rAction
    )  
    {
        var crashedWatchdog = new CrashedWatchdog(
            functionTypeId,
            rAction,
            _functionStore,
            _rFuncInvoker,
            _crashedCheckFrequency,
            _unhandledExceptionHandler
        );

        var postponedWatchdog = new PostponedWatchdog(
            functionTypeId,
            rAction,
            _functionStore,
            _rFuncInvoker,
            _postponedCheckFrequency,
            _unhandledExceptionHandler
        );

        _ = crashedWatchdog.Start();
        _ = postponedWatchdog.Start();
        
        return Tuple.Create(crashedWatchdog, postponedWatchdog);
    } 
}
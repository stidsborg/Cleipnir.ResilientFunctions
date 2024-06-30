using System;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class ReInvokerFactory
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly IFunctionStore _functionStore;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    
    private readonly AsyncSemaphore _asyncSemaphore;
    
    private readonly RestartFunction _restartFunction;
    private readonly ScheduleReInvokeFromWatchdog _scheduleReInvoke;

    public ReInvokerFactory(
        FunctionTypeId functionTypeId, 
        IFunctionStore functionStore,
        ShutdownCoordinator shutdownCoordinator, UnhandledExceptionHandler unhandledExceptionHandler, 
        TimeSpan checkFrequency, TimeSpan delayStartUp, 
        AsyncSemaphore asyncSemaphore, 
        RestartFunction restartFunction, ScheduleReInvokeFromWatchdog scheduleReInvoke
    )
    {
        _functionTypeId = functionTypeId;
        _functionStore = functionStore;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _checkFrequency = checkFrequency;
        _delayStartUp = delayStartUp;
        _asyncSemaphore = asyncSemaphore;
        _restartFunction = restartFunction;
        _scheduleReInvoke = scheduleReInvoke;
    }

    public ReInvoker Create(ReInvoker.GetEligibleFunctions getEligibleFunctions)
        => new ReInvoker(
            _functionTypeId,
            _functionStore,
            _shutdownCoordinator,
            _unhandledExceptionHandler,
            _checkFrequency,
            _delayStartUp,
            _asyncSemaphore,
            _restartFunction,
            _scheduleReInvoke,
            getEligibleFunctions
        );
}
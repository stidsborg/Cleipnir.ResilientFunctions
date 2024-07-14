using System;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class ReInvokerFactory
{
    private readonly FlowType _flowType;
    private readonly IFunctionStore _functionStore;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    
    private readonly AsyncSemaphore _asyncSemaphore;
    
    private readonly RestartFunction _restartFunction;
    private readonly ScheduleReInvokeFromWatchdog _scheduleReInvoke;

    public ReInvokerFactory(
        FlowType flowType, 
        IFunctionStore functionStore,
        ShutdownCoordinator shutdownCoordinator, UnhandledExceptionHandler unhandledExceptionHandler, 
        TimeSpan checkFrequency, TimeSpan delayStartUp, 
        AsyncSemaphore asyncSemaphore, 
        RestartFunction restartFunction, ScheduleReInvokeFromWatchdog scheduleReInvoke
    )
    {
        _flowType = flowType;
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
            _flowType,
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
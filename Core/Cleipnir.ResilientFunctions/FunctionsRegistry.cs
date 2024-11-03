using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.InnerAdapters;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public class FunctionsRegistry : IDisposable
{
    private readonly Dictionary<FlowType, object> _functions = new();

    private readonly IFunctionStore _functionStore;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly SettingsWithDefaults _settings;

    private readonly TimeoutWatchdog _timeoutWatchdog;
    private readonly CrashedOrPostponedWatchdog _crashedOrPostponedWatchdog;
    private readonly StoredTypes _storedTypes;
    
    private volatile bool _disposed;
    private readonly object _sync = new();
    
    public FunctionsRegistry(IFunctionStore functionStore, Settings? settings = null)
    {
        _functionStore = functionStore;
        _storedTypes = new StoredTypes(functionStore.TypeStore);
        _shutdownCoordinator = new ShutdownCoordinator();
        _settings = SettingsWithDefaults.Default.Merge(settings);

        _timeoutWatchdog = new TimeoutWatchdog(
            functionStore.TimeoutStore,
            _settings.WatchdogCheckFrequency,
            _settings.DelayStartup,
            _settings.UnhandledExceptionHandler,
            _shutdownCoordinator
        );
        _crashedOrPostponedWatchdog = new CrashedOrPostponedWatchdog(
            _functionStore,
            _shutdownCoordinator,
            _settings.UnhandledExceptionHandler,
            _settings.WatchdogCheckFrequency,
            _settings.DelayStartup
        );
    }

    // ** !! FUNC !! ** //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Task<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        flowType,
        InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
        settings
    );
    
    // ** W. WORKFLOW * //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Workflow, Task<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        flowType,
        InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
        settings
    );
    
    // ** W. RESULT ** //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Task<Result<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterFunc(
            flowType,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );

    // ** !! ACTION !! ** //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FlowType flowType,
        Func<TParam, Task> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            flowType,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );

    // ** W. WORKFLOW * //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FlowType flowType,
        Func<TParam, Workflow, Task> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            flowType,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    // ** W. RESULT ** //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FlowType flowType,
        Func<TParam, Task<Result>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            flowType,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
        
    // ** W. RESULT AND WORKFLOW ** //   
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FlowType flowType,
        Func<TParam, Workflow, Task<Result>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            flowType,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
        
    // ** PARAMLESS ** //   
    public ParamlessRegistration RegisterParamless(
        FlowType flowType,
        Func<Task<Result>> inner,
        Settings? settings = null
    ) => RegisterParamless(
        flowType,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );
        
    public ParamlessRegistration RegisterParamless(
        FlowType flowType,
        Func<Workflow, Task<Result>> inner,
        Settings? settings = null
    ) => RegisterParamless(
        flowType,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );
        
    public ParamlessRegistration RegisterParamless(
        FlowType flowType,
        Func<Task<Result<Unit>>> inner,
        Settings? settings = null
    ) => RegisterParamless(
        flowType,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );
        
    public ParamlessRegistration RegisterParamless(
        FlowType flowType,
        Func<Workflow, Task<Result<Unit>>> inner,
        Settings? settings = null
    ) => RegisterParamless(
        flowType,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );

    public ParamlessRegistration RegisterParamless(
        FlowType flowType,
        Func<Task> inner,
        Settings? settings = null
    ) => RegisterParamless(
        flowType,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );
        
    public ParamlessRegistration RegisterParamless(
        FlowType flowType,
        Func<Workflow, Task> inner,
        Settings? settings = null
    ) => RegisterParamless(
        flowType,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );
    
    // ** ASYNC W. RESULT AND WORKFLOW ** //   
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Workflow, Task<Result<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(FunctionsRegistry)} has been disposed");

        lock (_sync)
        {
            if (_functions.ContainsKey(flowType))
                return (FuncRegistration<TParam, TReturn>)_functions[flowType];
            
            var settingsWithDefaults = _settings.Merge(settings);
            var storedType = _storedTypes.InsertOrGet(flowType).GetAwaiter().GetResult();
            var invocationHelper = new InvocationHelper<TParam, TReturn>(
                flowType,
                storedType,
                isParamlessFunction: false,
                settingsWithDefaults,
                _functionStore,
                _shutdownCoordinator
            );
            var invoker = new Invoker<TParam, TReturn>(
                flowType, 
                storedType,
                inner,
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities
            );

            WatchDogsFactory.CreateAndStart(
                flowType,
                storedType,
                _functionStore,
                _timeoutWatchdog,
                _crashedOrPostponedWatchdog,
                invoker.Restart,
                invocationHelper.RestartFunction,
                invoker.ScheduleRestart,
                settingsWithDefaults,
                _shutdownCoordinator
            );

            var controlPanels = new ControlPanelFactory<TParam, TReturn>(
                flowType,
                storedType,
                invoker,
                invocationHelper
            );

            var messageWriters = new MessageWriters(
                storedType,
                _functionStore,
                settingsWithDefaults.Serializer,
                invoker.ScheduleRestart
            );

            var postman = new Postman(
                storedType,
                _functionStore.CorrelationStore,
                messageWriters
            );

            Task<IReadOnlyList<StoredInstance>> GetInstances(Status? status) =>
                status == null
                    ? _functionStore.GetInstances(storedType)
                    : _functionStore.GetInstances(storedType, status.Value);

            var registration = new FuncRegistration<TParam, TReturn>(
                flowType,
                storedType,
                invoker.Invoke,
                invoker.ScheduleInvoke,
                invoker.ScheduleAt,
                invocationHelper.BulkSchedule,
                GetInstances,
                controlPanels,
                messageWriters,
                new StateFetcher(storedType, _functionStore.EffectsStore, settingsWithDefaults.Serializer),
                postman
            );
            _functions[flowType] = registration;
            
            return registration;
        }
    }
    
    private ParamlessRegistration RegisterParamless(
        FlowType flowType,
        Func<Unit, Workflow, Task<Result<Unit>>> inner,
        Settings? settings = null
    ) 
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(FunctionsRegistry)} has been disposed");
        
        lock (_sync)
        {
            if (_functions.ContainsKey(flowType))
                return (ParamlessRegistration)_functions[flowType];
            
            var settingsWithDefaults = _settings.Merge(settings);
            var storedType = _storedTypes.InsertOrGet(flowType).GetAwaiter().GetResult();
            var invocationHelper = new InvocationHelper<Unit, Unit>(
                flowType,
                storedType,
                isParamlessFunction: true,
                settingsWithDefaults,
                _functionStore,
                _shutdownCoordinator
            );
            var invoker = new Invoker<Unit, Unit>(
                flowType, 
                storedType,
                inner, 
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities
            );
            
            WatchDogsFactory.CreateAndStart(
                flowType,
                storedType,
                _functionStore,
                _timeoutWatchdog,
                _crashedOrPostponedWatchdog,
                invoker.Restart,
                invocationHelper.RestartFunction,
                invoker.ScheduleRestart,
                settingsWithDefaults,
                _shutdownCoordinator
            );

            var controlPanels = new ControlPanelFactory(
                flowType,
                storedType,
                invoker,
                invocationHelper
            );

            var messageWriters = new MessageWriters(
                storedType,
                _functionStore,
                settingsWithDefaults.Serializer,
                invoker.ScheduleRestart
            );

            var postman = new Postman(
                storedType,
                _functionStore.CorrelationStore,
                messageWriters
            );
            
            Task<IReadOnlyList<StoredInstance>> GetInstances(Status? status) =>
                status == null
                    ? _functionStore.GetInstances(storedType)
                    : _functionStore.GetInstances(storedType, status.Value);

            var registration = new ParamlessRegistration(
                flowType,
                storedType,
                invoke: id => invoker.Invoke(id.Value, param: Unit.Instance),
                schedule: id => invoker.ScheduleInvoke(id.Value, param: Unit.Instance),
                scheduleAt: (id, at) => invoker.ScheduleAt(id.Value, param: Unit.Instance, at),
                bulkSchedule: ids => invocationHelper.BulkSchedule(ids.Select(id => new BulkWork<Unit>(id.Value, Unit.Instance))),
                GetInstances,
                controlPanels,
                messageWriters,
                new StateFetcher(storedType, _functionStore.EffectsStore, settingsWithDefaults.Serializer),
                postman
            );
            _functions[flowType] = registration;
            
            return registration;
        }
    }
    
    private ActionRegistration<TParam> RegisterAction<TParam>(
        FlowType flowType,
        Func<TParam, Workflow, Task<Result<Unit>>> inner,
        Settings? settings = null
    ) where TParam : notnull
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(FunctionsRegistry)} has been disposed");
        
        lock (_sync)
        {
            if (_functions.ContainsKey(flowType))
                return (ActionRegistration<TParam>)_functions[flowType];

            var storedType = _storedTypes.InsertOrGet(flowType).GetAwaiter().GetResult();
            var settingsWithDefaults = _settings.Merge(settings);
            var invocationHelper = new InvocationHelper<TParam, Unit>(
                flowType,
                storedType,
                isParamlessFunction: false,
                settingsWithDefaults,
                _functionStore,
                _shutdownCoordinator
            );
            var rActionInvoker = new Invoker<TParam, Unit>(
                flowType, 
                storedType,
                inner, 
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities
            );
            
            WatchDogsFactory.CreateAndStart(
                flowType,
                storedType,
                _functionStore,
                _timeoutWatchdog,
                _crashedOrPostponedWatchdog,
                rActionInvoker.Restart,
                invocationHelper.RestartFunction,
                rActionInvoker.ScheduleRestart,
                settingsWithDefaults,
                _shutdownCoordinator
            );

            var controlPanels = new ControlPanelFactory<TParam>(
                flowType,
                storedType,
                rActionInvoker,
                invocationHelper
            );

            var messageWriters = new MessageWriters(
                storedType,
                _functionStore,
                settingsWithDefaults.Serializer,
                rActionInvoker.ScheduleRestart
            );
            var postman = new Postman(
                storedType,
                _functionStore.CorrelationStore,
                messageWriters
            );
            
            Task<IReadOnlyList<StoredInstance>> GetInstances(Status? status) =>
                status == null
                    ? _functionStore.GetInstances(storedType)
                    : _functionStore.GetInstances(storedType, status.Value);
            
            var registration = new ActionRegistration<TParam>(
                flowType,
                storedType,
                rActionInvoker.Invoke,
                rActionInvoker.ScheduleInvoke,
                rActionInvoker.ScheduleAt,
                invocationHelper.BulkSchedule,
                GetInstances,
                controlPanels,
                messageWriters,
                new StateFetcher(storedType, _functionStore.EffectsStore, settingsWithDefaults.Serializer),
                postman
            );
            _functions[flowType] = registration;
            
            return registration;
        }
    }
    
    public void Dispose() => _ = ShutdownGracefully();

    public Task ShutdownGracefully(TimeSpan? maxWait = null)
    {
        _disposed = true;
        // ReSharper disable once InconsistentlySynchronizedField
        var shutdownTask = _shutdownCoordinator.PerformShutdown();
        if (maxWait == null)
            return shutdownTask;

        var tcs = new TaskCompletionSource();
        shutdownTask.ContinueWith(_ => tcs.TrySetResult());
            
        Task.Delay(maxWait.Value)
            .ContinueWith(_ =>
                tcs.TrySetException(new TimeoutException("Shutdown did not complete within threshold"))
            );

        return tcs.Task;
    }
}
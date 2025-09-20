using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
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
    
    private readonly CrashedOrPostponedWatchdog _crashedOrPostponedWatchdog;
    private readonly LeasesUpdater _leasesUpdater;
    private readonly StoredTypes _storedTypes;
    
    public ClusterInfo ClusterInfo { get; }
    
    private volatile bool _disposed;
    private readonly Lock _sync = new();
    private readonly ReplicaWatchdog _replicaWatchdog;

    public FunctionsRegistry(IFunctionStore functionStore, Settings? settings = null)
    {
        _functionStore = functionStore;
        _storedTypes = new StoredTypes(functionStore.TypeStore);
        _shutdownCoordinator = new ShutdownCoordinator();
        _settings = SettingsWithDefaults.Default.Merge(settings);
        var utcNow = _settings.UtcNow;
        _leasesUpdater = new LeasesUpdater(_settings.LeaseLength, _functionStore, _settings.UnhandledExceptionHandler, utcNow);
        _ = _leasesUpdater.Start();
        
        ClusterInfo = new ClusterInfo(ReplicaId.NewId());
        
        _crashedOrPostponedWatchdog = new CrashedOrPostponedWatchdog(
            _functionStore,
            _shutdownCoordinator,
            _settings.UnhandledExceptionHandler,
            _settings.WatchdogCheckFrequency,
            _settings.DelayStartup,
            ClusterInfo,
            _leasesUpdater, 
            utcNow
        );
        
        _replicaWatchdog = new ReplicaWatchdog(ClusterInfo, functionStore, leaseLength: TimeSpan.FromSeconds(1), utcNow, _settings.UnhandledExceptionHandler);
        _replicaWatchdog.Initialize().GetAwaiter().GetResult();
        _ = _replicaWatchdog.Start();
    }

    #region Func overloads

    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Task<TReturn>> inner,
        LocalSettings? settings = null
    ) where TParam : notnull => RegisterFunc(
        flowType,
        InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
        settings
    );
    
    // ** W. WORKFLOW * //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Workflow, Task<TReturn>> inner,
        LocalSettings? settings = null
    ) where TParam : notnull => RegisterFunc(
        flowType,
        InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
        settings
    );
    
    // ** W. RESULT ** //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Task<Result<TReturn>>> inner,
        LocalSettings? settings = null
    ) where TParam : notnull
        => RegisterFunc(
            flowType,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );

    #endregion

    #region Action overloads

    public ActionRegistration<TParam> RegisterAction<TParam>(
        FlowType flowType,
        Func<TParam, Task> inner,
        LocalSettings? settings = null
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
        LocalSettings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            flowType,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    // ** W. RESULT ** //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FlowType flowType,
        Func<TParam, Task<Result<Unit>>> inner,
        LocalSettings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            flowType,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );

    #endregion

    #region Paramless overloads

    // ** PARAMLESS ** //   
    public ParamlessRegistration RegisterParamless(
        FlowType flowType,
        Func<Task<Result<Unit>>> inner,
        LocalSettings? settings = null
    ) => RegisterParamless(
        flowType,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );
        
    public ParamlessRegistration RegisterParamless(
        FlowType flowType,
        Func<Workflow, Task<Result<Unit>>> inner,
        LocalSettings? settings = null
    ) => RegisterParamless(
        flowType,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );

    public ParamlessRegistration RegisterParamless(
        FlowType flowType,
        Func<Task> inner,
        LocalSettings? settings = null
    ) => RegisterParamless(
        flowType,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );
        
    public ParamlessRegistration RegisterParamless(
        FlowType flowType,
        Func<Workflow, Task> inner,
        LocalSettings? settings = null
    ) => RegisterParamless(
        flowType,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );

    #endregion
    
    // ** ASYNC W. RESULT AND WORKFLOW ** //   
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Workflow, Task<Result<TReturn>>> inner,
        LocalSettings? settings = null
    ) where TParam : notnull
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(FunctionsRegistry)} has been disposed");

        lock (_sync)
        {
            if (_functions.ContainsKey(flowType))
                return (FuncRegistration<TParam, TReturn>)_functions[flowType];
            
            var settingsWithDefaults = _settings.Merge(settings);
            var serializer = settingsWithDefaults
                .Serializer
                .DecorateWithCustomerSerializableHandling()
                .DecorateWithErrorHandling();
            
            var storedType = _storedTypes.InsertOrGet(flowType).GetAwaiter().GetResult();
            var invocationHelper = new InvocationHelper<TParam, TReturn>(
                flowType,
                storedType,
                ClusterInfo.ReplicaId,
                isParamlessFunction: false,
                settingsWithDefaults,
                _functionStore,
                _shutdownCoordinator,
                _leasesUpdater,
                serializer,
                _settings.UtcNow
            );
            var invoker = new Invoker<TParam, TReturn>(
                flowType, 
                storedType,
                inner,
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities,
                ClusterInfo.ReplicaId
            );

            WatchDogsFactory.CreateAndStart(
                flowType,
                storedType,
                _functionStore,
                _crashedOrPostponedWatchdog,
                invoker.Restart,
                invocationHelper.RestartFunction,
                invoker.ScheduleRestart,
                settingsWithDefaults,
                _shutdownCoordinator,
                serializer,
                _settings.UtcNow
            );

            var controlPanels = new ControlPanelFactory<TParam, TReturn>(
                flowType,
                storedType,
                invoker,
                invocationHelper,
                _settings.UtcNow
            );

            var messageWriters = new MessageWriters(
                storedType,
                _functionStore,
                serializer,
                invoker.ScheduleRestart
            );

            var postman = new Postman(
                storedType,
                _functionStore.CorrelationStore,
                messageWriters
            );
            
            var registration = new FuncRegistration<TParam, TReturn>(
                flowType,
                storedType,
                _functionStore,
                invoker.Invoke,
                schedule: async (instance, param, detach, initialState) => (await invoker.ScheduleInvoke(instance, param, detach, initialState)).ToScheduledWithResult(),
                scheduleAt: async (instance, param, until, detach) => (await invoker.ScheduleAt(instance, param, until, detach) ).ToScheduledWithResult(),
                bulkSchedule: async (instances, detach) => (await invocationHelper.BulkSchedule(instances.ToList(), detach)).ToScheduledWithResults(),
                controlPanels,
                messageWriters,
                new StateFetcher(storedType, _functionStore.EffectsStore, serializer),
                postman,
                _settings.UtcNow
            );
            _functions[flowType] = registration;
            
            return registration;
        }
    }
    
    private ParamlessRegistration RegisterParamless(
        FlowType flowType,
        Func<Unit, Workflow, Task<Result<Unit>>> inner,
        LocalSettings? settings = null
    ) 
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(FunctionsRegistry)} has been disposed");
        
        lock (_sync)
        {
            if (_functions.ContainsKey(flowType))
                return (ParamlessRegistration)_functions[flowType];
            
            var settingsWithDefaults = _settings.Merge(settings);
            var serializer = settingsWithDefaults
                .Serializer
                .DecorateWithCustomerSerializableHandling()
                .DecorateWithErrorHandling();
            var storedType = _storedTypes.InsertOrGet(flowType).GetAwaiter().GetResult();
            var invocationHelper = new InvocationHelper<Unit, Unit>(
                flowType,
                storedType,
                ClusterInfo.ReplicaId,
                isParamlessFunction: true,
                settingsWithDefaults,
                _functionStore,
                _shutdownCoordinator,
                _leasesUpdater,
                serializer,
                _settings.UtcNow
            );
            var invoker = new Invoker<Unit, Unit>(
                flowType, 
                storedType,
                inner, 
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities,
                ClusterInfo.ReplicaId
            );
            
            WatchDogsFactory.CreateAndStart(
                flowType,
                storedType,
                _functionStore,
                _crashedOrPostponedWatchdog,
                invoker.Restart,
                invocationHelper.RestartFunction,
                invoker.ScheduleRestart,
                settingsWithDefaults,
                _shutdownCoordinator,
                serializer,
                _settings.UtcNow
            );

            var controlPanels = new ControlPanelFactory(
                flowType,
                storedType,
                invoker,
                invocationHelper,
                _settings.UtcNow
            );

            var messageWriters = new MessageWriters(
                storedType,
                _functionStore,
                serializer,
                invoker.ScheduleRestart
            );

            var postman = new Postman(
                storedType,
                _functionStore.CorrelationStore,
                messageWriters
            );

            var registration = new ParamlessRegistration(
                flowType,
                storedType,
                _functionStore,
                invoke: (id, initialState) => invoker.Invoke(id.Value, param: Unit.Instance, initialState),
                schedule: async (id, detach, initialState) => (await invoker.ScheduleInvoke(id.Value, param: Unit.Instance, detach, initialState)).ToScheduledWithoutResult(),
                scheduleAt: async (id, at, detach) => (await invoker.ScheduleAt(id.Value, param: Unit.Instance, at, detach)).ToScheduledWithoutResult(),
                bulkSchedule: async (ids, detach) => (await invocationHelper.BulkSchedule(ids.Select(id => new BulkWork<Unit>(id.Value, Unit.Instance)).ToList(), detach)).ToScheduledWithoutResults(),
                controlPanels,
                messageWriters,
                new StateFetcher(storedType, _functionStore.EffectsStore, serializer),
                postman,
                _settings.UtcNow
            );
            _functions[flowType] = registration;
            
            return registration;
        }
    }
    
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FlowType flowType,
        Func<TParam, Workflow, Task<Result<Unit>>> inner,
        LocalSettings? settings = null
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
            var serializer = settingsWithDefaults
                .Serializer
                .DecorateWithCustomerSerializableHandling()
                .DecorateWithErrorHandling();
            var invocationHelper = new InvocationHelper<TParam, Unit>(
                flowType,
                storedType,
                ClusterInfo.ReplicaId,
                isParamlessFunction: false,
                settingsWithDefaults,
                _functionStore,
                _shutdownCoordinator,
                _leasesUpdater,
                serializer,
                _settings.UtcNow
            );
            var rActionInvoker = new Invoker<TParam, Unit>(
                flowType, 
                storedType,
                inner, 
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities,
                ClusterInfo.ReplicaId
            );
            
            WatchDogsFactory.CreateAndStart(
                flowType,
                storedType,
                _functionStore,
                _crashedOrPostponedWatchdog,
                rActionInvoker.Restart,
                invocationHelper.RestartFunction,
                rActionInvoker.ScheduleRestart,
                settingsWithDefaults,
                _shutdownCoordinator,
                serializer,
                _settings.UtcNow
            );

            var controlPanels = new ControlPanelFactory<TParam>(
                flowType,
                storedType,
                rActionInvoker,
                invocationHelper,
                _settings.UtcNow
            );

            var messageWriters = new MessageWriters(
                storedType,
                _functionStore,
                serializer,
                rActionInvoker.ScheduleRestart
            );
            var postman = new Postman(
                storedType,
                _functionStore.CorrelationStore,
                messageWriters
            );
            
            var registration = new ActionRegistration<TParam>(
                flowType,
                storedType,
                _functionStore,
                rActionInvoker.Invoke,
                schedule: async (instance, param, detach, initialState) => (await rActionInvoker.ScheduleInvoke(instance, param, detach, initialState)).ToScheduledWithoutResult(), 
                scheduleAt: async (instance, param, until, completion) => (await rActionInvoker.ScheduleAt(instance, param, until, completion)).ToScheduledWithoutResult(), 
                bulkSchedule: async (instances, detach) => (await invocationHelper.BulkSchedule(instances.ToList(), detach)).ToScheduledWithoutResults(),
                controlPanels,
                messageWriters,
                new StateFetcher(storedType, _functionStore.EffectsStore, serializer),
                postman,
                _settings.UtcNow
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
        shutdownTask.ContinueWith(_ =>
        {
            _replicaWatchdog.Dispose();
            tcs.TrySetResult();
        });
            
        Task.Delay(maxWait.Value)
            .ContinueWith(_ =>
                tcs.TrySetException(new TimeoutException("Shutdown did not complete within threshold"))
            );
        
        return tcs.Task;
    }
}
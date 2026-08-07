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
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public class FunctionsRegistry : IDisposable
{
    private readonly Dictionary<FlowType, object> _functions = new();

    private readonly IFunctionStore _functionStore;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly SettingsWithDefaults _settings;
    
    private readonly PostponedWatchdog _postponedWatchdog;
    private readonly StoredTypes _storedTypes;
    private readonly TypeMapper _typeMapper;
    
    public ClusterInfo ClusterInfo { get; }
    public DlqManager DeadLetterQueue { get; }
    
    private volatile bool _disposed;
    private bool _started;
    private readonly Lock _sync = new();
    private readonly ReplicaWatchdog _replicaWatchdog;
    private readonly MessageWatchdog _messageWatchdog;
    private readonly MessageSender _messageSender;
    private readonly MessageClearer _messageClearer;
    private readonly FlowsManagers _flowsManagers;
    private readonly MessageDeserializer _messageDeserializer;

    private FunctionsRegistry(IFunctionStore functionStore, Settings? settings = null)
    {
        _functionStore = functionStore;
        _storedTypes = new StoredTypes(functionStore.FlowTypeStore);
        _shutdownCoordinator = new ShutdownCoordinator();
        _settings = SettingsWithDefaults.Default.Merge(settings);
        // One mapper for the whole registry: types are encoded by simple qualified name (serializer-independent),
        // so type ids and their persisted encodings are registry-wide facts.
        _typeMapper = new TypeMapper(functionStore.TypeStore);
        var utcNow = _settings.UtcNow;
        _messageClearer = new MessageClearer(
            _functionStore.MessageStore,
            _settings.UnhandledExceptionHandler,
            _settings.WatchdogCheckFrequency
        );
        ClusterInfo = new ClusterInfo(ReplicaId.NewId());

        _messageSender = new MessageSender(
            _functionStore,
            _settings.Serializer.DecorateWithErrorHandling(),
            ClusterInfo
        );

        DeadLetterQueue = new DlqManager(
            _functionStore.DlqStore,
            _messageSender,
            _storedTypes,
            _messageClearer,
            _settings.UnhandledExceptionHandler,
            _settings.UnregisteredFlowTypesGracePeriod
        );

        _flowsManagers = new FlowsManagers(
            _functionStore,
            _messageClearer,
            ClusterInfo
        );

        // A single deserializer for the whole registry: the serializer is registry-global (per-registration
        // settings cannot override it), so nothing about deserialization is type-specific.
        _messageDeserializer = new MessageDeserializer(
            _settings.Serializer.DecorateWithErrorHandling(),
            _functionStore.DlqStore,
            _messageClearer,
            _settings.UnhandledExceptionHandler
        );

        _postponedWatchdog = new PostponedWatchdog(
            _functionStore,
            _messageSender,
            _shutdownCoordinator,
            _settings.UnhandledExceptionHandler,
            _settings.WatchdogCheckFrequency,
            ClusterInfo,
            utcNow
        );

        _replicaWatchdog = new ReplicaWatchdog(
            ClusterInfo,
            functionStore,
            heartbeatFrequency: _settings.ReplicaHeartbeatFrequency,
            utcNow,
            _settings.UnhandledExceptionHandler
        );

        // The MessageWatchdog is the message-delivery loop, so it runs at the message-pull frequency - the
        // (slower) watchdog check frequency would make every push-restarted exchange poll-bound.
        _messageWatchdog = new MessageWatchdog(
            _functionStore.MessageStore,
            _flowsManagers,
            _messageDeserializer,
            DeadLetterQueue,
            _messageClearer,
            ClusterInfo,
            _shutdownCoordinator,
            _settings.UnhandledExceptionHandler,
            _settings.MessagesPullFrequency,
            utcNow
        );

        // Property-injected last: the sender notifies the watchdog while the watchdog's collaborators send
        // through the sender - wiring the notify target after construction is what breaks the cycle.
        _messageSender.MessageWatchdog = _messageWatchdog;
    }

    /// <summary>
    /// Creates a <see cref="FunctionsRegistry"/>, invokes <paramref name="setup"/> to register the application's
    /// flow types and only then starts background processing - cluster membership, message delivery and
    /// crash/postponed recovery. The registry is sealed once started: registering a flow type afterwards throws.
    /// Messages fetched for types not registered on this replica are held for <see cref="Settings"/>'
    /// UnregisteredFlowTypesGracePeriod and then moved to the dead letter queue.
    /// </summary>
    public static Task<FunctionsRegistry> CreateAndStart(
        IFunctionStore functionStore,
        Action<FunctionsRegistry> setup)
        => CreateAndStart(functionStore, settings: null, setup);

    /// <inheritdoc cref="CreateAndStart(IFunctionStore,Action{FunctionsRegistry})"/>
    public static async Task<FunctionsRegistry> CreateAndStart(
        IFunctionStore functionStore,
        Settings? settings,
        Action<FunctionsRegistry> setup)
    {
        var registry = new FunctionsRegistry(functionStore, settings);
        setup(registry);
        await registry.Start();
        return registry;
    }

    /// <summary>
    /// Value-returning counterpart to <see cref="CreateAndStart(IFunctionStore,Action{FunctionsRegistry})"/>:
    /// returns the registry together with <paramref name="setup"/>'s return value, so the typed registrations
    /// escape to the caller without having to be assigned to captured locals.
    /// </summary>
    public static Task<(FunctionsRegistry Registry, T Flows)> CreateAndStart<T>(
        IFunctionStore functionStore,
        Func<FunctionsRegistry, T> setup)
        => CreateAndStart(functionStore, settings: null, setup);

    /// <inheritdoc cref="CreateAndStart{T}(IFunctionStore,Func{FunctionsRegistry,T})"/>
    public static async Task<(FunctionsRegistry Registry, T Flows)> CreateAndStart<T>(
        IFunctionStore functionStore,
        Settings? settings,
        Func<FunctionsRegistry, T> setup)
    {
        var registry = new FunctionsRegistry(functionStore, settings);
        var flows = setup(registry);
        await registry.Start();
        return (registry, flows);
    }

    private async Task Start()
    {
        // Seal before any background loop runs. Registration takes the same lock, so every registration made in
        // the setup delegate happens-before this write - and therefore before the loops started below observe the
        // registered flow types. This is what lets FlowsManagers read its dictionary without synchronization.
        lock (_sync)
            _started = true;

        // The replica must join the cluster (replica insert + offset calculation) before any loop that shards
        // by cluster offset or claims flows for this replica is allowed to run.
        await _replicaWatchdog.Start();
        _postponedWatchdog.Start();
        _ = Task.Run(_messageWatchdog.Start);
    }

    /// <summary>
    /// Guards the registration entry points. Must be called while holding <see cref="_sync"/>.
    /// </summary>
    private void ThrowIfStarted()
    {
        if (_started)
            throw new InvalidOperationException(
                $"Flow types cannot be registered after the {nameof(FunctionsRegistry)} has been started - " +
                $"register all flow types in the setup delegate passed to {nameof(CreateAndStart)}"
            );
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
    internal FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
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
    internal ActionRegistration<TParam> RegisterAction<TParam>(
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
    internal ParamlessRegistration RegisterParamless(
        FlowType flowType,
        Func<Task<Result<Unit>>> inner,
        LocalSettings? settings = null
    ) => RegisterParamless(
        flowType,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );
        
    internal ParamlessRegistration RegisterParamless(
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
    internal FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Workflow, Task<Result<TReturn>>> inner,
        LocalSettings? settings = null
    ) where TParam : notnull
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(FunctionsRegistry)} has been disposed");

        lock (_sync)
        {
            ThrowIfStarted();

            if (_functions.ContainsKey(flowType))
                return (FuncRegistration<TParam, TReturn>)_functions[flowType];
            
            var settingsWithDefaults = _settings.Merge(settings);
            var serializer = settingsWithDefaults
                .Serializer
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
                serializer,
                _typeMapper,
                _settings.UtcNow,
                settings?.ClearChildrenAfterCapture ?? true,
                _messageClearer,
                _messageSender,
                _messageDeserializer
            );
            var invoker = new Invoker<TParam, TReturn>(
                flowType,
                storedType,
                inner,
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                ClusterInfo.ReplicaId,
                _flowsManagers
            );
            _flowsManagers.Create(storedType, invoker);

            WatchDogsFactory.CreateAndStart(
                flowType,
                storedType,
                _functionStore,
                _postponedWatchdog,
                settingsWithDefaults,
                _shutdownCoordinator,
                _settings.UtcNow
            );

            var controlPanels = new ControlPanelFactory<TParam, TReturn>(
                flowType,
                storedType,
                invocationHelper,
                _settings.UtcNow
            );

            var registration = new FuncRegistration<TParam, TReturn>(
                flowType,
                storedType,
                invoker,
                controlPanels,
                _messageSender,
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
            ThrowIfStarted();

            if (_functions.ContainsKey(flowType))
                return (ParamlessRegistration)_functions[flowType];
            
            var settingsWithDefaults = _settings.Merge(settings);
            var serializer = settingsWithDefaults
                .Serializer
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
                serializer,
                _typeMapper,
                _settings.UtcNow,
                settings?.ClearChildrenAfterCapture ?? true,
                _messageClearer,
                _messageSender,
                _messageDeserializer
            );
            var invoker = new Invoker<Unit, Unit>(
                flowType,
                storedType,
                inner,
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                ClusterInfo.ReplicaId,
                _flowsManagers
            );
            _flowsManagers.Create(storedType, invoker);

            WatchDogsFactory.CreateAndStart(
                flowType,
                storedType,
                _functionStore,
                _postponedWatchdog,
                settingsWithDefaults,
                _shutdownCoordinator,
                _settings.UtcNow
            );

            var controlPanels = new ControlPanelFactory(
                flowType,
                storedType,
                invocationHelper,
                _settings.UtcNow
            );

            var registration = new ParamlessRegistration(
                flowType,
                storedType,
                _functionStore,
                invoker,
                controlPanels,
                _messageSender,
                _settings.UtcNow
            );
            _functions[flowType] = registration;
            
            return registration;
        }
    }
    
    internal ActionRegistration<TParam> RegisterAction<TParam>(
        FlowType flowType,
        Func<TParam, Workflow, Task<Result<Unit>>> inner,
        LocalSettings? settings = null
    ) where TParam : notnull
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(FunctionsRegistry)} has been disposed");
        
        lock (_sync)
        {
            ThrowIfStarted();

            if (_functions.ContainsKey(flowType))
                return (ActionRegistration<TParam>)_functions[flowType];

            var storedType = _storedTypes.InsertOrGet(flowType).GetAwaiter().GetResult();
            var settingsWithDefaults = _settings.Merge(settings);
            var serializer = settingsWithDefaults
                .Serializer
                .DecorateWithErrorHandling();
            var invocationHelper = new InvocationHelper<TParam, Unit>(
                flowType,
                storedType,
                ClusterInfo.ReplicaId,
                isParamlessFunction: false,
                settingsWithDefaults,
                _functionStore,
                _shutdownCoordinator,
                serializer,
                _typeMapper,
                _settings.UtcNow,
                settings?.ClearChildrenAfterCapture ?? true,
                _messageClearer,
                _messageSender,
                _messageDeserializer
            );
            var rActionInvoker = new Invoker<TParam, Unit>(
                flowType,
                storedType,
                inner,
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                ClusterInfo.ReplicaId,
                _flowsManagers
            );
            _flowsManagers.Create(storedType, rActionInvoker);

            WatchDogsFactory.CreateAndStart(
                flowType,
                storedType,
                _functionStore,
                _postponedWatchdog,
                settingsWithDefaults,
                _shutdownCoordinator,
                _settings.UtcNow
            );

            var controlPanels = new ControlPanelFactory<TParam>(
                flowType,
                storedType,
                invocationHelper,
                _settings.UtcNow
            );

            var registration = new ActionRegistration<TParam>(
                flowType,
                storedType,
                rActionInvoker,
                controlPanels,
                _messageSender,
                _settings.UtcNow
            );
            _functions[flowType] = registration;
            
            return registration;
        }
    }

    public void Dispose()
    {
        _disposed = true;
        _shutdownCoordinator.SignalShutdown();
        _replicaWatchdog.Dispose();
    }

    public Task ShutdownGracefully(TimeSpan? maxWait = null)
    {
        _disposed = true;
        
        // ReSharper disable once InconsistentlySynchronizedField
        var shutdownTask = _shutdownCoordinator.PerformShutdown();
        if (maxWait == null)
            return shutdownTask.ContinueWith(t =>
            {
                _replicaWatchdog.Dispose();
                return t;
            });
        
        var tcs = new TaskCompletionSource();
        shutdownTask.ContinueWith(_ => tcs.TrySetResult());
            
        Task.Delay(maxWait.Value)
            .ContinueWith(_ =>
                tcs.TrySetException(new TimeoutException("Shutdown did not complete within threshold"))
            );

        tcs.Task.ContinueWith(_ => _replicaWatchdog.Dispose());
        
        return tcs.Task;
    }
}
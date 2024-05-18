using System;
using System.Collections.Generic;
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
    private readonly Dictionary<FunctionTypeId, object> _functions = new();

    private readonly IFunctionStore _functionStore;
    public IFunctionStore FunctionStore => _functionStore;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly SettingsWithDefaults _settings;
    
    private volatile bool _disposed;
    private readonly object _sync = new();
    
    public FunctionsRegistry(IFunctionStore functionStore, Settings? settings = null)
    {
        _functionStore = functionStore;
        _shutdownCoordinator = new ShutdownCoordinator();
        _settings = SettingsWithDefaults.Default.Merge(settings);
    }

    // ** !! FUNC !! ** //
    // ** SYNC ** //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TReturn> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. WORKFLOW ** //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Workflow, TReturn> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC ** //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
        settings
    );
    
    // ** ASYNC W. WORKFLOW * //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Workflow, Task<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
        settings
    );

    // ** SYNC W. RESULT ** //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Result<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
        settings
    );
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Workflow, Result<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );
   
    // ** ASYNC W. RESULT ** //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<Result<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );

    // ** !! ACTION !! ** //
    // ** SYNC ** //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Action<TParam> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );

    // ** SYNC W. WORKFLOW ** //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Action<TParam, Workflow> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC ** //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );

    // ** ASYNC W. WORKFLOW * //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Workflow, Task> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. RESULT ** //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Result> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Workflow, Result> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
        // ** ASYNC W. RESULT ** //
        public ActionRegistration<TParam> RegisterAction<TParam>(
            FunctionTypeId functionTypeId,
            Func<TParam, Task<Result>> inner,
            Settings? settings = null
        ) where TParam : notnull
            => RegisterAction(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                settings
            );
        
        // ** ASYNC W. RESULT AND WORKFLOW ** //   
        public ActionRegistration<TParam> RegisterAction<TParam>(
            FunctionTypeId functionTypeId,
            Func<TParam, Workflow, Task<Result>> inner,
            Settings? settings = null
        ) where TParam : notnull
            => RegisterAction(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                settings
            );
        
    // ** PARAMLESS ** //   
    public ParamlessRegistration RegisterParamless(
        FunctionTypeId functionTypeId,
        Func<Task<Result>> inner,
        Settings? settings = null
    ) => RegisterParamless(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );
        
    public ParamlessRegistration RegisterParamless(
        FunctionTypeId functionTypeId,
        Func<Workflow, Task<Result>> inner,
        Settings? settings = null
    ) => RegisterParamless(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );
        
    public ParamlessRegistration RegisterParamless(
        FunctionTypeId functionTypeId,
        Func<Task<Result<Unit>>> inner,
        Settings? settings = null
    ) => RegisterParamless(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );
        
    public ParamlessRegistration RegisterParamless(
        FunctionTypeId functionTypeId,
        Func<Workflow, Task<Result<Unit>>> inner,
        Settings? settings = null
    ) => RegisterParamless(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );

    public ParamlessRegistration RegisterParamless(
        FunctionTypeId functionTypeId,
        Func<Task> inner,
        Settings? settings = null
    ) => RegisterParamless(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );
        
    public ParamlessRegistration RegisterParamless(
        FunctionTypeId functionTypeId,
        Func<Workflow, Task> inner,
        Settings? settings = null
    ) => RegisterParamless(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerParamlessWithTaskResultReturn(inner),
        settings
    );
    
    // ** ASYNC W. RESULT AND WORKFLOW ** //   
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Workflow, Task<Result<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(FunctionsRegistry)} has been disposed");

        lock (_sync)
        {
            if (_functions.ContainsKey(functionTypeId))
                return (FuncRegistration<TParam, TReturn>)_functions[functionTypeId];
            
            var settingsWithDefaults = _settings.Merge(settings);
            var invocationHelper = new InvocationHelper<TParam, TReturn>(isNullParamAllowed: false, settingsWithDefaults, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new Invoker<TParam, TReturn>(
                functionTypeId, 
                inner,
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities,
                GetMessageWriter
            );

            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                rFuncInvoker.ReInvoke,
                settingsWithDefaults,
                _shutdownCoordinator
            );

            var controlPanels = new ControlPanelFactory<TParam, TReturn>(
                functionTypeId,
                rFuncInvoker,
                invocationHelper
            );
            var registration = new FuncRegistration<TParam, TReturn>(
                functionTypeId,
                rFuncInvoker.Invoke,
                rFuncInvoker.ScheduleInvoke,
                rFuncInvoker.ScheduleAt,
                controlPanels,
                new MessageWriters(functionTypeId, _functionStore, settingsWithDefaults.Serializer, rFuncInvoker.ScheduleReInvoke),
                new StateFetcher(_functionStore, settingsWithDefaults.Serializer)
            );
            _functions[functionTypeId] = registration;
            
            return registration;
        }
    }
    
    private ParamlessRegistration RegisterParamless(
        FunctionTypeId functionTypeId,
        Func<Unit, Workflow, Task<Result<Unit>>> inner,
        Settings? settings = null
    ) 
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(FunctionsRegistry)} has been disposed");
        
        lock (_sync)
        {
            if (_functions.ContainsKey(functionTypeId))
                return (ParamlessRegistration)_functions[functionTypeId];
            
            var settingsWithDefaults = _settings.Merge(settings);
            var invocationHelper = new InvocationHelper<Unit, Unit>(isNullParamAllowed: true, settingsWithDefaults, _functionStore, _shutdownCoordinator);
            var invoker = new Invoker<Unit, Unit>(
                functionTypeId, 
                inner, 
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities,
                GetMessageWriter
            );
            
            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                invoker.ReInvoke,
                settingsWithDefaults,
                _shutdownCoordinator
            );

            var controlPanels = new ControlPanelFactory(
                functionTypeId,
                invoker,
                invocationHelper
            );
            var registration = new ParamlessRegistration(
                functionTypeId,
                invoke: id => invoker.Invoke(id.Value, param: Unit.Instance),
                schedule: id => invoker.ScheduleInvoke(id.Value, param: Unit.Instance),
                scheduleAt: (id, at) => invoker.ScheduleAt(id.Value, param: Unit.Instance, at),
                controlPanels,
                new MessageWriters(functionTypeId, _functionStore, settingsWithDefaults.Serializer, invoker.ScheduleReInvoke),
                new StateFetcher(_functionStore, settingsWithDefaults.Serializer)
            );
            _functions[functionTypeId] = registration;
            
            return registration;
        }
    }
    
    private ActionRegistration<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Workflow, Task<Result<Unit>>> inner,
        Settings? settings = null
    ) where TParam : notnull
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(FunctionsRegistry)} has been disposed");
        
        lock (_sync)
        {
            if (_functions.ContainsKey(functionTypeId))
                return (ActionRegistration<TParam>)_functions[functionTypeId];
            
            var settingsWithDefaults = _settings.Merge(settings);
            var invocationHelper = new InvocationHelper<TParam, Unit>(isNullParamAllowed: false, settingsWithDefaults, _functionStore, _shutdownCoordinator);
            var rActionInvoker = new Invoker<TParam, Unit>(
                functionTypeId, 
                inner, 
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities,
                GetMessageWriter
            );
            
            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                rActionInvoker.ReInvoke,
                settingsWithDefaults,
                _shutdownCoordinator
            );

            var controlPanels = new ControlPanelFactory<TParam>(
                functionTypeId,
                rActionInvoker,
                invocationHelper
            );
            var registration = new ActionRegistration<TParam>(
                functionTypeId,
                rActionInvoker.Invoke,
                rActionInvoker.ScheduleInvoke,
                rActionInvoker.ScheduleAt,
                controlPanels,
                new MessageWriters(functionTypeId, _functionStore, settingsWithDefaults.Serializer, rActionInvoker.ScheduleReInvoke),
                new StateFetcher(_functionStore, settingsWithDefaults.Serializer)
            );
            _functions[functionTypeId] = registration;
            
            return registration;
        }
    }

    public MessageWriter GetMessageWriter(FunctionId functionId)
    {
        dynamic? registration = null;
        lock (_sync)
            if (_functions.ContainsKey(functionId.TypeId))
                registration = _functions[functionId.TypeId];

        if (registration == null)
            throw new ArgumentException($"Cannot create {nameof(MessageWriter)} for unregistered function type '{functionId.TypeId}'");
        
        return (MessageWriter) registration.MessageWriters.For(functionId.InstanceId);
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
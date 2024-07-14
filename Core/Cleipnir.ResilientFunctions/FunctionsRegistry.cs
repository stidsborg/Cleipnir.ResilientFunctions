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
    public IFunctionStore FunctionStore => _functionStore;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly SettingsWithDefaults _settings;

    private readonly Dictionary<FlowType, List<RoutingInformation>> _routes = new();
    
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
        FlowType flowType,
        Func<TParam, TReturn> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterFunc(
            flowType,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. WORKFLOW ** //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Workflow, TReturn> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterFunc(
            flowType,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC ** //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Task<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        flowType,
        InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
        settings
    );
    
    // ** ASYNC W. WORKFLOW * //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Workflow, Task<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        flowType,
        InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
        settings
    );

    // ** SYNC W. RESULT ** //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Result<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        flowType,
        InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
        settings
    );
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    public FuncRegistration<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FlowType flowType,
        Func<TParam, Workflow, Result<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterFunc(
            flowType,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );
   
    // ** ASYNC W. RESULT ** //
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
    // ** SYNC ** //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FlowType flowType,
        Action<TParam> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            flowType,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );

    // ** SYNC W. WORKFLOW ** //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FlowType flowType,
        Action<TParam, Workflow> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            flowType,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC ** //
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

    // ** ASYNC W. WORKFLOW * //
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
    
    // ** SYNC W. RESULT ** //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FlowType flowType,
        Func<TParam, Result> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            flowType,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    public ActionRegistration<TParam> RegisterAction<TParam>(
        FlowType flowType,
        Func<TParam, Workflow, Result> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            flowType,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
        // ** ASYNC W. RESULT ** //
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
        
        // ** ASYNC W. RESULT AND WORKFLOW ** //   
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
            var invocationHelper = new InvocationHelper<TParam, TReturn>(
                flowType,
                isParamlessFunction: false,
                settingsWithDefaults,
                _functionStore,
                _shutdownCoordinator
            );
            var rFuncInvoker = new Invoker<TParam, TReturn>(
                flowType, 
                inner,
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities,
                GetMessageWriter
            );

            WatchDogsFactory.CreateAndStart(
                flowType,
                _functionStore,
                rFuncInvoker.ReInvoke,
                invocationHelper.RestartFunction,
                rFuncInvoker.ScheduleReInvoke,
                settingsWithDefaults,
                _shutdownCoordinator
            );

            var controlPanels = new ControlPanelFactory<TParam, TReturn>(
                flowType,
                rFuncInvoker,
                invocationHelper
            );
            var registration = new FuncRegistration<TParam, TReturn>(
                flowType,
                rFuncInvoker.Invoke,
                rFuncInvoker.ScheduleInvoke,
                rFuncInvoker.ScheduleAt,
                invocationHelper.BulkSchedule,
                controlPanels,
                new MessageWriters(flowType, _functionStore, settingsWithDefaults.Serializer, rFuncInvoker.ScheduleReInvoke),
                new StateFetcher(_functionStore, settingsWithDefaults.Serializer)
            );
            _functions[flowType] = registration;

            if (settingsWithDefaults.Routes.Any())
                _routes[flowType] = settingsWithDefaults.Routes.ToList();
            
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
            var invocationHelper = new InvocationHelper<Unit, Unit>(
                flowType,
                isParamlessFunction: true,
                settingsWithDefaults,
                _functionStore,
                _shutdownCoordinator
            );
            var invoker = new Invoker<Unit, Unit>(
                flowType, 
                inner, 
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities,
                GetMessageWriter
            );
            
            WatchDogsFactory.CreateAndStart(
                flowType,
                _functionStore,
                invoker.ReInvoke,
                invocationHelper.RestartFunction,
                invoker.ScheduleReInvoke,
                settingsWithDefaults,
                _shutdownCoordinator
            );

            var controlPanels = new ControlPanelFactory(
                flowType,
                invoker,
                invocationHelper
            );
            var registration = new ParamlessRegistration(
                flowType,
                invoke: id => invoker.Invoke(id.Value, param: Unit.Instance),
                schedule: id => invoker.ScheduleInvoke(id.Value, param: Unit.Instance),
                scheduleAt: (id, at) => invoker.ScheduleAt(id.Value, param: Unit.Instance, at),
                bulkSchedule: ids => invocationHelper.BulkSchedule(ids.Select(id => new BulkWork<Unit>(id, Unit.Instance))),
                controlPanels,
                new MessageWriters(flowType, _functionStore, settingsWithDefaults.Serializer, invoker.ScheduleReInvoke),
                new StateFetcher(_functionStore, settingsWithDefaults.Serializer)
            );
            _functions[flowType] = registration;
            
            if (settingsWithDefaults.Routes.Any())
                _routes[flowType] = settingsWithDefaults.Routes.ToList();
            
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
            
            var settingsWithDefaults = _settings.Merge(settings);
            var invocationHelper = new InvocationHelper<TParam, Unit>(
                flowType,
                isParamlessFunction: false,
                settingsWithDefaults,
                _functionStore,
                _shutdownCoordinator
            );
            var rActionInvoker = new Invoker<TParam, Unit>(
                flowType, 
                inner, 
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities,
                GetMessageWriter
            );
            
            WatchDogsFactory.CreateAndStart(
                flowType,
                _functionStore,
                rActionInvoker.ReInvoke,
                invocationHelper.RestartFunction,
                rActionInvoker.ScheduleReInvoke,
                settingsWithDefaults,
                _shutdownCoordinator
            );

            var controlPanels = new ControlPanelFactory<TParam>(
                flowType,
                rActionInvoker,
                invocationHelper
            );
            var registration = new ActionRegistration<TParam>(
                flowType,
                rActionInvoker.Invoke,
                rActionInvoker.ScheduleInvoke,
                rActionInvoker.ScheduleAt,
                invocationHelper.BulkSchedule,
                controlPanels,
                new MessageWriters(flowType, _functionStore, settingsWithDefaults.Serializer, rActionInvoker.ScheduleReInvoke),
                new StateFetcher(_functionStore, settingsWithDefaults.Serializer)
            );
            _functions[flowType] = registration;
            
            if (settingsWithDefaults.Routes.Any())
                _routes[flowType] = settingsWithDefaults.Routes.ToList();
            
            return registration;
        }
    }

    public MessageWriter GetMessageWriter(FlowId flowId)
    {
        dynamic? registration = null;
        lock (_sync)
            if (_functions.ContainsKey(flowId.Type))
                registration = _functions[flowId.Type];

        if (registration == null)
            throw new ArgumentException($"Cannot create {nameof(MessageWriter)} for unregistered function type '{flowId.Type}'");
        
        return (MessageWriter) registration.MessageWriters.For(flowId.Instance);
    }

    public async Task DeliverMessage(object message, Type messageType)
    {
        Dictionary<FlowType, Tuple<RouteResolver, MessageWriters>> resolvers;
        
        lock (_sync)
        {
            resolvers = _routes
                .SelectMany(kv =>
                    kv.Value.Select(ri => new { FunctionTypeId = kv.Key, RouteInfo = ri })
                )
                .Where(a => a.RouteInfo.MessageType == messageType)
                .ToDictionary(
                    a => a.FunctionTypeId, 
                    a => 
                        Tuple.Create(
                            a.RouteInfo.RouteResolver,
                            (MessageWriters) ((dynamic) _functions[a.FunctionTypeId]).MessageWriters
                        )
                );
            
        }

        foreach (var (functionTypeId, (routeResolver, messageWriters)) in resolvers)
        {
            var routingInfo = routeResolver(message);
            if (routingInfo.CorrelationId is not null)
            {
                var functionIds = await _functionStore
                    .CorrelationStore
                    .GetCorrelations(routingInfo.CorrelationId);

                foreach (var (typeId, instanceId) in functionIds)
                {
                    var messageWriter = messageWriters.For(instanceId);
                    var finding = await messageWriter.AppendMessage(message, routingInfo.IdempotencyKey);
                    if (finding == Finding.NotFound)
                        await ScheduleIfParamless(typeId, instanceId);
                }
            }
            else
            {
                var messageWriter = messageWriters.For(routingInfo.FlowInstanceId!);
                var finding = await messageWriter.AppendMessage(message, routingInfo.IdempotencyKey);   
                
                if (finding == Finding.NotFound)
                    await ScheduleIfParamless(functionTypeId, routingInfo.FlowInstanceId!);
            }
        }
    }

    public async Task DeliverMessage<TMessage>(TMessage message) where TMessage : notnull
        => await DeliverMessage(message, typeof(TMessage)); 

    private async Task ScheduleIfParamless(FlowType flowType, FlowInstance flowInstance)
    {
        ParamlessRegistration paramlessRegistration;
        lock (_sync)
        {
            if (_functions[flowType] is ParamlessRegistration registration)
                paramlessRegistration = registration;
            else
                return;
        }

        await paramlessRegistration.Schedule(flowInstance);
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
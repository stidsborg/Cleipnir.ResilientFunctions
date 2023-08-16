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

public class RFunctions : IDisposable
{
    private delegate Task ReInvokeResilientFunction(string functionInstanceId, int expectedEpoch);
    private delegate Task ScheduleResilientFunctionReInvocation(string functionInstanceId, int expectedEpoch);
    
    private readonly Dictionary<FunctionTypeId, object> _functions = new();
    private readonly Dictionary<FunctionTypeId, ReInvokeResilientFunction> _reInvokes = new();
    private readonly Dictionary<FunctionTypeId, ScheduleResilientFunctionReInvocation> _scheduleReInvocations = new();

    private readonly IFunctionStore _functionStore;
    private readonly IEventStore _eventStore;
    public IFunctionStore FunctionStore => _functionStore;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly SettingsWithDefaults _settings;
    
    private volatile bool _disposed;
    private readonly object _sync = new();
    
    public RFunctions(IFunctionStore functionStore, Settings? settings = null)
    {
        _functionStore = functionStore;
        _eventStore = functionStore.EventStore;
        _shutdownCoordinator = new ShutdownCoordinator();
        _settings = SettingsWithDefaults.Default.Merge(settings);
    }

    // ** !! FUNC !! ** //
    // ** SYNC ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TReturn> inner,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                settings
            )
        );
    
    // ** SYNC W. CONTEXT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Context, TReturn> inner,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                settings
            )
        );
    
    // ** ASYNC ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                settings
            )
        );
    
    // ** ASYNC W. CONTEXT * //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Context, Task<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                settings
            )
        );

    // ** SYNC W. RESULT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Result<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                settings
            )
        );
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Context, Result<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                settings
            )
        );
   
    // ** ASYNC W. RESULT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<Result<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => new RFunc<TParam, TReturn>(
                RegisterFunc(
                    functionTypeId,
                    InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                    settings
                )
            );

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Context, Task<Result<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => new RFunc<TParam, TReturn>(
                RegisterFunc(
                    functionTypeId,
                    InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                    settings
                )
            );

    // ** !! ACTION !! ** //
    // ** SYNC ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Action<TParam> inner,
        Settings? settings = null
    ) where TParam : notnull
        => new RAction<TParam>(
                RegisterAction(
                    functionTypeId,
                    InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                    settings
                )
            );

    // ** SYNC W. CONTEXT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Action<TParam, Context> inner,
        Settings? settings = null
    ) where TParam : notnull
        => new RAction<TParam>(
            RegisterAction(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                settings
            )
        );
    
    // ** ASYNC ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task> inner,
        Settings? settings = null
    ) where TParam : notnull
        => new RAction<TParam>(
            RegisterAction(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                settings
            )
        );

    // ** ASYNC W. CONTEXT * //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Context, Task> inner,
        Settings? settings = null
    ) where TParam : notnull
        => new RAction<TParam>(
            RegisterAction(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                settings
            )
        );
    
    // ** SYNC W. RESULT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Result> inner,
        Settings? settings = null
    ) where TParam : notnull
        => new RAction<TParam>(
            RegisterAction(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                settings
            )
        );
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Context, Result> inner,
        Settings? settings = null
    ) where TParam : notnull
        => new RAction<TParam>(
            RegisterAction(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                settings
            )
        );
    
        // ** ASYNC W. RESULT ** //
        public RAction<TParam> RegisterAction<TParam>(
            FunctionTypeId functionTypeId,
            Func<TParam, Task<Result>> inner,
            Settings? settings = null
        ) where TParam : notnull
            => new RAction<TParam>(
                RegisterAction(
                    functionTypeId,
                    InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                    settings
                )
            );
        
        // ** ASYNC W. RESULT AND CONTEXT ** //   
        public RAction<TParam> RegisterAction<TParam>(
            FunctionTypeId functionTypeId,
            Func<TParam, Context, Task<Result>> inner,
            Settings? settings = null
        ) where TParam : notnull
            => new RAction<TParam>(
                RegisterAction(
                    functionTypeId,
                    InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                    settings
                )
            );

    // ** !! FUNC WITH SCRAPBOOK !! ** //
    
    // ** SYNC ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, TReturn> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );

    // ** SYNC W. CONTEXT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, TReturn> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC W. CONTEXT * //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Task<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. RESULT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Result<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Result<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC W. RESULT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC W. RESULT AND CONTEXT ** //   
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");

        lock (_sync)
        {
            if (_functions.ContainsKey(functionTypeId))
            {
                if (_functions[functionTypeId] is not RFunc<TParam, TScrapbook, TReturn> r)
                    throw new ArgumentException($"<{typeof(RFunc<TParam, TScrapbook, TReturn>).SimpleQualifiedName()}> is not compatible with existing {_functions[functionTypeId].GetType().SimpleQualifiedName()}");
                return r;
            }
        
            var settingsWithDefaults = _settings.Merge(settings);
            var invocationHelper = new InvocationHelper<TParam, TScrapbook, TReturn>(settingsWithDefaults, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new Invoker<TParam, TScrapbook, TReturn>(
                functionTypeId, 
                inner,
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities,
                settingsWithDefaults.PostponedCheckFrequency
            );

            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                reInvoke: (id, epoch, status) => rFuncInvoker.ReInvoke(id.ToString(), epoch, status),
                settingsWithDefaults,
                _shutdownCoordinator
            );

            var controlPanels = new ControlPanels<TParam, TScrapbook, TReturn>(
                functionTypeId,
                rFuncInvoker,
                invocationHelper
            );
            var registration = new RFunc<TParam, TScrapbook, TReturn>(
                rFuncInvoker.Invoke,
                rFuncInvoker.ScheduleInvoke,
                rFuncInvoker.ScheduleAt,
                controlPanels,
                new EventSourceWriters(functionTypeId, _functionStore, settingsWithDefaults.Serializer, rFuncInvoker.ScheduleReInvoke)
            );
            _functions[functionTypeId] = registration;
            _reInvokes[functionTypeId] = (id, epoch) => rFuncInvoker.ReInvoke(id, epoch);
            _scheduleReInvocations[functionTypeId] = (id, epoch) => rFuncInvoker.ScheduleReInvoke(id, epoch);
            
            return registration;
        }
    }

    // ** !! ACTION WITH SCRAPBOOK !! ** //
    // ** SYNC ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Action<TParam, TScrapbook> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. CONTEXT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Action<TParam, TScrapbook, Context> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC W. CONTEXT * //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Task> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. RESULT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Result> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Result> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC W. RESULT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    internal RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Task<Result>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            settings
        );
    
    private RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Task<Result<Unit>>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
        
        lock (_sync)
        {
            if (_functions.ContainsKey(functionTypeId))
            {
                if (_functions[functionTypeId] is not RAction<TParam, TScrapbook> r)
                    throw new ArgumentException($"<{typeof(RAction<TParam, TScrapbook>).SimpleQualifiedName()}> is not compatible with existing {_functions[functionTypeId].GetType().SimpleQualifiedName()}");
                return r;
            }

            var settingsWithDefaults = _settings.Merge(settings);
            var invocationHelper = new InvocationHelper<TParam, TScrapbook, Unit>(settingsWithDefaults, _functionStore, _shutdownCoordinator);
            var rActionInvoker = new Invoker<TParam, TScrapbook, Unit>(
                functionTypeId, 
                inner, 
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler,
                _functionStore.Utilities,
                settingsWithDefaults.PostponedCheckFrequency
            );
            
            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                (id, epoch, status) => rActionInvoker.ReInvoke(id.ToString(), epoch, status),
                settingsWithDefaults,
                _shutdownCoordinator
            );

            var controlPanels = new ControlPanels<TParam, TScrapbook>(
                functionTypeId,
                rActionInvoker,
                invocationHelper
            );
            var registration = new RAction<TParam, TScrapbook>(
                rActionInvoker.Invoke,
                rActionInvoker.ScheduleInvoke,
                rActionInvoker.ScheduleAt,
                controlPanels,
                new EventSourceWriters(functionTypeId, _functionStore, settingsWithDefaults.Serializer, rActionInvoker.ScheduleReInvoke)
            );
            _functions[functionTypeId] = registration;
            _reInvokes[functionTypeId] = (id, epoch) => rActionInvoker.ReInvoke(id, epoch);
            _scheduleReInvocations[functionTypeId] = (id, epoch) => rActionInvoker.ScheduleReInvoke(id, epoch);
            return registration;
        }
    }

    public Task ReInvoke(string functionTypeId, string functionInstanceId, int expectedEpoch)
    {
        ReInvokeResilientFunction reInvoke;
        lock (_sync)
        {
            if (!_reInvokes.ContainsKey(functionTypeId)) 
                throw new InvalidOperationException($"FunctionType '{functionTypeId}' has not been registered");

            reInvoke = _reInvokes[functionTypeId];
        }

        return reInvoke(functionInstanceId, expectedEpoch);
    }

    public Task ScheduleReInvoke(string functionTypeId, string functionInstanceId, int expectedEpoch)
    {
        ScheduleResilientFunctionReInvocation scheduleReInvocation;
        lock (_sync)
        {
            if (!_scheduleReInvocations.ContainsKey(functionTypeId)) 
                throw new InvalidOperationException($"FunctionType '{functionTypeId}' has not been registered");
            
            scheduleReInvocation = _scheduleReInvocations[functionTypeId];
        }

        return scheduleReInvocation(functionInstanceId, expectedEpoch);
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
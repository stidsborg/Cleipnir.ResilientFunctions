using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.InnerAdapters;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public class RFunctions : IDisposable
{
    private delegate Task ReInvokeResilientFunction(string functionInstanceId, IEnumerable<Status> expectedStatus, int? expectedEpoch);
    private delegate Task ScheduleResilientFunctionReInvocation(
        string functionInstanceId, 
        IEnumerable<Status> expectedStatus, 
        int? expectedEpoch
    );
    
    private readonly Dictionary<FunctionTypeId, object> _functions = new();
    private readonly Dictionary<FunctionTypeId, ReInvokeResilientFunction> _reInvokes = new();
    private readonly Dictionary<FunctionTypeId, ScheduleResilientFunctionReInvocation> _scheduleReInvocations = new();

    private readonly IFunctionStore _functionStore;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly SettingsWithDefaults _settings;
    
    private volatile bool _disposed;
    private readonly object _sync = new();
    
    public RFunctions(IFunctionStore functionStore, Settings? settings = null)
    {
        _functionStore = functionStore;
        _shutdownCoordinator = new ShutdownCoordinator();
        _settings = SettingsWithDefaults.Default.Merge(settings);
    }

    // ** !! FUNC !! ** //
    // ** SYNC ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TReturn> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                version,
                settings
            )
        );
    
    // ** SYNC W. CONTEXT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Context, TReturn> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                version,
                settings
            )
        );
    
    // ** ASYNC ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<TReturn>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                version,
                settings
            )
        );
    
    // ** ASYNC W. CONTEXT * //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Context, Task<TReturn>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                version,
                settings
            )
        );

    // ** SYNC W. RESULT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Result<TReturn>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                version,
                settings
            )
        );
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Context, Result<TReturn>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                version,
                settings
            )
        );
   
    // ** ASYNC W. RESULT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<Result<TReturn>>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => new RFunc<TParam, TReturn>(
                RegisterFunc(
                    functionTypeId,
                    InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                    version,
                    settings
                )
            );

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Context, Task<Result<TReturn>>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => new RFunc<TParam, TReturn>(
                RegisterFunc(
                    functionTypeId,
                    InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
                    version,
                    settings
                )
            );

    // ** !! ACTION !! ** //
    // ** SYNC ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Action<TParam> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => new RAction<TParam>(
                RegisterAction(
                    functionTypeId,
                    InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                    version,
                    settings
                )
            );

    // ** SYNC W. CONTEXT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Action<TParam, Context> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => new RAction<TParam>(
            RegisterAction(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                version,
                settings
            )
        );
    
    // ** ASYNC ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => new RAction<TParam>(
            RegisterAction(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                version,
                settings
            )
        );

    // ** ASYNC W. CONTEXT * //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Context, Task> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => new RAction<TParam>(
            RegisterAction(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                version,
                settings
            )
        );
    
    // ** SYNC W. RESULT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Result> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => new RAction<TParam>(
            RegisterAction(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                version,
                settings
            )
        );
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Context, Result> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => new RAction<TParam>(
            RegisterAction(
                functionTypeId,
                InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                version,
                settings
            )
        );
    
        // ** ASYNC W. RESULT ** //
        public RAction<TParam> RegisterAction<TParam>(
            FunctionTypeId functionTypeId,
            Func<TParam, Task<Result>> inner,
            int version = 0,
            Settings? settings = null
        ) where TParam : notnull
            => new RAction<TParam>(
                RegisterAction(
                    functionTypeId,
                    InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                    version,
                    settings
                )
            );
        
        // ** ASYNC W. RESULT AND CONTEXT ** //   
        public RAction<TParam> RegisterAction<TParam>(
            FunctionTypeId functionTypeId,
            Func<TParam, Context, Task<Result>> inner,
            int version = 0,
            Settings? settings = null
        ) where TParam : notnull
            => new RAction<TParam>(
                RegisterAction(
                    functionTypeId,
                    InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
                    version,
                    settings
                )
            );

    // ** !! FUNC WITH SCRAPBOOK !! ** //
    
    // ** SYNC ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, TReturn> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );

    // ** SYNC W. CONTEXT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, TReturn> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** ASYNC ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<TReturn>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** ASYNC W. CONTEXT * //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Task<TReturn>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** SYNC W. RESULT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Result<TReturn>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Result<TReturn>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** ASYNC W. RESULT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerFuncWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** ASYNC W. RESULT AND CONTEXT ** //   
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
        if (concreteScrapbookType != null && !concreteScrapbookType.IsSubclassOf(typeof(TScrapbook)))
            throw new ArgumentException($"Concrete scrapbook type '{concreteScrapbookType.FullName}' must be child of '{typeof(TScrapbook).FullName}'");

        lock (_sync)
        {
            if (_functions.ContainsKey(functionTypeId))
            {
                if (_functions[functionTypeId] is not RFunc<TParam, TScrapbook, TReturn> r)
                    throw new ArgumentException($"<{typeof(RFunc<TParam, TScrapbook, TReturn>).SimpleQualifiedName()}> is not compatible with existing {_functions[functionTypeId].GetType().SimpleQualifiedName()}");
                return r;
            }
        
            var settingsWithDefaults = _settings.Merge(settings);
            var invocationHelper = new InvocationHelper<TParam, TScrapbook, TReturn>(settingsWithDefaults, version, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new Invoker<Unit, TParam, TScrapbook, TReturn>(
                functionTypeId, 
                inner, 
                innerMethodSelector: null,
                settingsWithDefaults.DependencyResolver,
                new MiddlewarePipeline(settingsWithDefaults.Middlewares),
                concreteScrapbookType,  
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler
            );

            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                reInvoke: (id, statuses, epoch) => rFuncInvoker.ReInvoke(id.ToString(), statuses, epoch),
                settingsWithDefaults,
                version,
                _shutdownCoordinator
            );

            var registration = new RFunc<TParam, TScrapbook, TReturn>(
                functionTypeId,
                invocationHelper,
                rFuncInvoker.Invoke,
                rFuncInvoker.ReInvoke,
                rFuncInvoker.ScheduleInvocation,
                rFuncInvoker.ScheduleReInvoke
            );
            _functions[functionTypeId] = registration;
            _reInvokes[functionTypeId] = (id, status, epoch) => rFuncInvoker.ReInvoke(id, status, epoch);
            _scheduleReInvocations[functionTypeId] = (id, status, epoch) 
                => rFuncInvoker.ScheduleReInvoke(id, status, epoch);
            
            return registration;
        }
    }

    // ** !! ACTION WITH SCRAPBOOK !! ** //
    // ** SYNC ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Action<TParam, TScrapbook> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** SYNC W. CONTEXT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Action<TParam, TScrapbook, Context> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** ASYNC ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** ASYNC W. CONTEXT * //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Task> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** SYNC W. RESULT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Result> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Result> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** ASYNC W. RESULT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    internal RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Task<Result>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerActionWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    private RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Task<Result<Unit>>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
        if (concreteScrapbookType != null && !concreteScrapbookType.IsSubclassOf(typeof(TScrapbook)))
            throw new ArgumentException($"Concrete scrapbook type '{concreteScrapbookType.FullName}' must be child of '{typeof(TScrapbook).FullName}'");
        
        lock (_sync)
        {
            if (_functions.ContainsKey(functionTypeId))
            {
                if (_functions[functionTypeId] is not RAction<TParam, TScrapbook> r)
                    throw new ArgumentException($"<{typeof(RAction<TParam, TScrapbook>).SimpleQualifiedName()}> is not compatible with existing {_functions[functionTypeId].GetType().SimpleQualifiedName()}");
                return r;
            }

            var settingsWithDefaults = _settings.Merge(settings);
            var invocationHelper = new InvocationHelper<TParam, TScrapbook, Unit>(settingsWithDefaults, version, _functionStore, _shutdownCoordinator);
            var rActionInvoker = new Invoker<Unit, TParam, TScrapbook, Unit>(
                functionTypeId, 
                inner, 
                innerMethodSelector: null,
                settingsWithDefaults.DependencyResolver,
                new MiddlewarePipeline(settingsWithDefaults.Middlewares),
                concreteScrapbookType,
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler
            );
            
            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                (id, statuses, epoch) => rActionInvoker.ReInvoke(id.ToString(), statuses, epoch),
                settingsWithDefaults,
                version,
                _shutdownCoordinator
            );

            var registration = new RAction<TParam, TScrapbook>(
                functionTypeId,
                invocationHelper,
                rActionInvoker.Invoke,
                rActionInvoker.ReInvoke,
                rActionInvoker.ScheduleInvocation,
                rActionInvoker.ScheduleReInvoke
            );
            _functions[functionTypeId] = registration;
            _reInvokes[functionTypeId] = (id, status, epoch) => rActionInvoker.ReInvoke(id, status, epoch);
            _scheduleReInvocations[functionTypeId] = (id, status, epoch) 
                => rActionInvoker.ScheduleReInvoke(id, status, epoch);
            return registration;
        }
    }

    // ** !! METHOD FUNC REGISTRATION !! ** //
    public MethodRegistrationBuilder<TEntity> RegisterMethod<TEntity>() where TEntity : notnull => new(this);
    
    // ** !! METHOD ACTION REGISTRATION !! ** //
    internal RAction<TParam> RegisterMethodAction<TEntity, TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<Unit>>>> innerMethodSelector,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull where TEntity : notnull
    {
        var rAction = RegisterMethodAction<TEntity, TParam, RScrapbook>(
            functionTypeId,
            innerMethodSelector,
            version,
            settings
        );
        return new RAction<TParam>(rAction);
    }
    
    // ** !! METHOD FUNC WITH SCRAPBOOK REGISTRATION !! ** //
    internal RFunc<TParam, TScrapbook, TReturn> RegisterMethodFunc<TEntity, TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>> innerMethodSelector,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() where TEntity : notnull
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
            if (settingsWithDefaults.DependencyResolver == null)
                throw new ArgumentNullException(nameof(IDependencyResolver), $"Cannot register method when settings' {nameof(IDependencyResolver)} is null");
            
            var invocationHelper = new InvocationHelper<TParam, TScrapbook, TReturn>(settingsWithDefaults, version, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new Invoker<TEntity, TParam, TScrapbook, TReturn>(
                functionTypeId, 
                inner: null,
                innerMethodSelector, 
                settingsWithDefaults.DependencyResolver,
                new MiddlewarePipeline(settingsWithDefaults.Middlewares),
                concreteScrapbookType,
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler
            );

            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                reInvoke: (id, statuses, epoch) => rFuncInvoker.ReInvoke(id.ToString(), statuses, epoch),
                settingsWithDefaults,
                version,
                _shutdownCoordinator
            );

            var registration = new RFunc<TParam, TScrapbook, TReturn>(
                functionTypeId,
                invocationHelper,
                rFuncInvoker.Invoke,
                rFuncInvoker.ReInvoke,
                rFuncInvoker.ScheduleInvocation,
                rFuncInvoker.ScheduleReInvoke
            );
            _functions[functionTypeId] = registration;
            return registration;
        }
    }

    // ** !! METHOD ACTION WITH SCRAPBOOK REGISTRATION !! ** //
    internal RAction<TParam, TScrapbook> RegisterMethodAction<TEntity, TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<Unit>>>> innerMethodSelector,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() where TEntity : notnull
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
            if (settingsWithDefaults.DependencyResolver == null)
                throw new ArgumentNullException(nameof(IDependencyResolver), $"Cannot register method when settings' {nameof(IDependencyResolver)} is null");
            
            var invocationHelper = new InvocationHelper<TParam, TScrapbook, Unit>(settingsWithDefaults, version, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new Invoker<TEntity, TParam, TScrapbook, Unit>(
                functionTypeId, 
                inner: null,
                innerMethodSelector,
                settingsWithDefaults.DependencyResolver,
                new MiddlewarePipeline(settingsWithDefaults.Middlewares),
                concreteScrapbookType,
                invocationHelper,
                settingsWithDefaults.UnhandledExceptionHandler
            );

            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                reInvoke: (id, statuses, epoch) => rFuncInvoker.ReInvoke(id.ToString(), statuses, epoch),
                settingsWithDefaults,
                version,
                _shutdownCoordinator
            );

            var registration = new RAction<TParam, TScrapbook>(
                functionTypeId,
                invocationHelper,
                rFuncInvoker.Invoke,
                rFuncInvoker.ReInvoke,
                rFuncInvoker.ScheduleInvocation,
                rFuncInvoker.ScheduleReInvoke
            );
            _functions[functionTypeId] = registration;
            return registration;
        }
    }

    public Task ReInvoke(
        string functionTypeId, 
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null
    )
    {
        ReInvokeResilientFunction reInvoke;
        lock (_sync)
        {
            if (!_reInvokes.ContainsKey(functionTypeId)) 
                throw new InvalidOperationException($"FunctionType '{functionTypeId}' has not been registered");

            reInvoke = _reInvokes[functionTypeId];
        }

        return reInvoke(functionInstanceId, expectedStatuses, expectedEpoch);
    }

    public Task ScheduleReInvoke(
        string functionTypeId,
        string functionInstanceId, 
        IEnumerable<Status> expectedStatuses, 
        int? expectedEpoch = null
    )
    {
        ScheduleResilientFunctionReInvocation scheduleReInvocation;
        lock (_sync)
        {
            if (!_scheduleReInvocations.ContainsKey(functionTypeId)) 
                throw new InvalidOperationException($"FunctionType '{functionTypeId}' has not been registered");
            
            scheduleReInvocation = _scheduleReInvocations[functionTypeId];
        }

        return scheduleReInvocation(
            functionInstanceId,
            expectedStatuses,
            expectedEpoch
        );
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
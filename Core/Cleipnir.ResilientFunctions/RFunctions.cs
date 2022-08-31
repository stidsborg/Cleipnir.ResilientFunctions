using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.InnerDecorators;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Watchdogs;

namespace Cleipnir.ResilientFunctions;

public class RFunctions : IDisposable 
{
    private readonly Dictionary<FunctionTypeId, object> _functions = new();

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

    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TReturn> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        settings
    );
    
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<TReturn>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        settings
    );
    
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Result<TReturn>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        settings        
    );
    
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<Result<TReturn>>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");

        lock (_sync)
        {
            if (_functions.ContainsKey(functionTypeId))
            {
                if (_functions[functionTypeId] is not RFunc<TParam, TReturn> r)
                    throw new ArgumentException($"{typeof(RFunc<TParam, TReturn>).SimpleQualifiedName()}> is not compatible with existing {_functions[functionTypeId].GetType().SimpleQualifiedName()}");
                return r;
            }

            var settingsWithDefaults = _settings.Merge(settings);
            var commonInvoker = new CommonInvoker(settingsWithDefaults, version, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new RFuncInvoker<TParam, TReturn>(
                functionTypeId, 
                inner, 
                new MiddlewarePipeline(settingsWithDefaults.Middlewares),
                settingsWithDefaults.DependencyResolver,
                commonInvoker,
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

            var registration = new RFunc<TParam, TReturn>(
                rFuncInvoker.Invoke,
                rFuncInvoker.ReInvoke,
                rFuncInvoker.ScheduleInvocation,
                rFuncInvoker.ScheduleReInvoke
            );
            _functions[functionTypeId] = registration;
            return registration;
        }
    }

    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings
        );
    
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Action<TParam> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings
        );
    
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Result> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings
        );
    
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<Result>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull 
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
        
        lock (_sync)
        {
            if (_functions.ContainsKey(functionTypeId))
            {
                if (_functions[functionTypeId] is not RAction<TParam> r)
                    throw new ArgumentException($"{typeof(RAction<TParam>).SimpleQualifiedName()}> is not compatible with existing {_functions[functionTypeId].GetType().SimpleQualifiedName()}");
                return r;
            }

            var settingsWithDefaults = _settings.Merge(settings);
            var commonInvoker = new CommonInvoker(settingsWithDefaults, version, _functionStore, _shutdownCoordinator);
            var rActionInvoker = new RActionInvoker<TParam>(
                functionTypeId, 
                inner, 
                new MiddlewarePipeline(settingsWithDefaults.Middlewares),
                _settings.DependencyResolver,
                commonInvoker,
                settingsWithDefaults.UnhandledExceptionHandler
            );

            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                reInvoke: (id, statuses, epoch) => rActionInvoker.ReInvoke(id.ToString(), statuses, epoch),
                settingsWithDefaults,
                version,
                _shutdownCoordinator
            );

            var registration =  new RAction<TParam>(
                rActionInvoker.Invoke,
                rActionInvoker.ReInvoke,
                rActionInvoker.ScheduleInvocation,
                rActionInvoker.ScheduleReInvoke
            );
            _functions[functionTypeId] = registration;
            return registration;
        }
    }

    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, TReturn> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );

    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<TReturn>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );

    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Result<TReturn>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );

    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner,
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
                    throw new ArgumentException($"{typeof(RFunc<TParam, TScrapbook, TReturn>).SimpleQualifiedName()}> is not compatible with existing {_functions[functionTypeId].GetType().SimpleQualifiedName()}");
                return r;
            }
        
            var settingsWithDefaults = _settings.Merge(settings);
            var commonInvoker = new CommonInvoker(settingsWithDefaults, version, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new RFuncInvoker<TParam, TScrapbook, TReturn>(
                functionTypeId, 
                inner, 
                concreteScrapbookType,
                new MiddlewarePipeline(settingsWithDefaults.Middlewares),
                settingsWithDefaults.DependencyResolver,
                commonInvoker,
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
                rFuncInvoker.Invoke,
                rFuncInvoker.ReInvoke,
                rFuncInvoker.ScheduleInvocation,
                rFuncInvoker.ScheduleReInvoke
            );
            _functions[functionTypeId] = registration;
            return registration;
        }
    }

    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Action<TParam, TScrapbook> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Result> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );

    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );

    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result>> inner,
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
                    throw new ArgumentException($"{typeof(RAction<TParam, TScrapbook>).SimpleQualifiedName()}> is not compatible with existing {_functions[functionTypeId].GetType().SimpleQualifiedName()}");
                return r;
            }

            var settingsWithDefaults = _settings.Merge(settings);
            var commonInvoker = new CommonInvoker(settingsWithDefaults, version, _functionStore, _shutdownCoordinator);
            var rActionInvoker = new RActionInvoker<TParam, TScrapbook>(
                functionTypeId, 
                inner, 
                concreteScrapbookType,
                new MiddlewarePipeline(settingsWithDefaults.Middlewares),
                settingsWithDefaults.DependencyResolver,
                commonInvoker,
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
                rActionInvoker.Invoke,
                rActionInvoker.ReInvoke,
                rActionInvoker.ScheduleInvocation,
                rActionInvoker.ScheduleReInvoke
            );
            _functions[functionTypeId] = registration;
            return registration;
        }
    }

    public MethodRegistrationBuilder<TEntity> RegisterMethod<TEntity>() where TEntity : notnull => new(this);

    internal RFunc<TParam, TReturn> RegisterMethodFunc<TEntity, TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task<Result<TReturn>>>> innerMethodSelector,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull where TEntity : notnull
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");

        lock (_sync)
        {
            if (_functions.ContainsKey(functionTypeId))
            {
                if (_functions[functionTypeId] is not RFunc<TParam, TReturn> r)
                    throw new ArgumentException($"{typeof(RFunc<TParam, TReturn>).SimpleQualifiedName()}> is not compatible with existing {_functions[functionTypeId].GetType().SimpleQualifiedName()}");
                return r;
            }

            var settingsWithDefaults = _settings.Merge(settings);
            if (settingsWithDefaults.DependencyResolver == null)
                throw new ArgumentNullException(nameof(IDependencyResolver), $"Cannot register method when settings' {nameof(IDependencyResolver)} is null");
            var commonInvoker = new CommonInvoker(settingsWithDefaults, version, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new RFuncMethodInvoker<TEntity, TParam, TReturn>(
                functionTypeId, 
                innerMethodSelector, 
                settingsWithDefaults.DependencyResolver,
                new MiddlewarePipeline(settingsWithDefaults.Middlewares),
                commonInvoker,
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

            var registration = new RFunc<TParam, TReturn>(
                rFuncInvoker.Invoke,
                rFuncInvoker.ReInvoke,
                rFuncInvoker.ScheduleInvocation,
                rFuncInvoker.ScheduleReInvoke
            );
            _functions[functionTypeId] = registration;
            return registration;
        }
    }
    
    internal RAction<TParam> RegisterMethodAction<TEntity, TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task<Result>>> innerMethodSelector,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull where TEntity : notnull
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");

        lock (_sync)
        {
            if (_functions.ContainsKey(functionTypeId))
            {
                if (_functions[functionTypeId] is not RAction<TParam> r)
                    throw new ArgumentException($"{typeof(RAction<TParam>).SimpleQualifiedName()}> is not compatible with existing {_functions[functionTypeId].GetType().SimpleQualifiedName()}");
                return r;
            }

            var settingsWithDefaults = _settings.Merge(settings);
            if (settingsWithDefaults.DependencyResolver == null)
                throw new ArgumentNullException(nameof(IDependencyResolver), $"Cannot register method when settings' {nameof(IDependencyResolver)} is null");
            
            var commonInvoker = new CommonInvoker(settingsWithDefaults, version, _functionStore, _shutdownCoordinator);
            var rActionInvoker = new RActionMethodInvoker<TEntity, TParam>(
                functionTypeId, 
                innerMethodSelector, 
                settingsWithDefaults.DependencyResolver,
                new MiddlewarePipeline(settingsWithDefaults.Middlewares),
                commonInvoker,
                settingsWithDefaults.UnhandledExceptionHandler
            );

            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                reInvoke: (id, statuses, epoch) => rActionInvoker.ReInvoke(id.ToString(), statuses, epoch),
                settingsWithDefaults,
                version,
                _shutdownCoordinator
            );

            var registration = new RAction<TParam>(
                rActionInvoker.Invoke,
                rActionInvoker.ReInvoke,
                rActionInvoker.ScheduleInvocation,
                rActionInvoker.ScheduleReInvoke
            );
            _functions[functionTypeId] = registration;
            return registration;
        }
    }
    
    internal RFunc<TParam, TScrapbook, TReturn> RegisterMethodFunc<TEntity, TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task<Result<TReturn>>>> innerMethodSelector,
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
                    throw new ArgumentException($"{typeof(RFunc<TParam, TScrapbook, TReturn>).SimpleQualifiedName()}> is not compatible with existing {_functions[functionTypeId].GetType().SimpleQualifiedName()}");
                return r;
            }

            var settingsWithDefaults = _settings.Merge(settings);
            if (settingsWithDefaults.DependencyResolver == null)
                throw new ArgumentNullException(nameof(IDependencyResolver), $"Cannot register method when settings' {nameof(IDependencyResolver)} is null");
            
            var commonInvoker = new CommonInvoker(settingsWithDefaults, version, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new RFuncMethodInvoker<TEntity, TParam, TScrapbook, TReturn>(
                functionTypeId, 
                innerMethodSelector, 
                settingsWithDefaults.DependencyResolver,
                new MiddlewarePipeline(settingsWithDefaults.Middlewares),
                concreteScrapbookType,
                commonInvoker,
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
                rFuncInvoker.Invoke,
                rFuncInvoker.ReInvoke,
                rFuncInvoker.ScheduleInvocation,
                rFuncInvoker.ScheduleReInvoke
            );
            _functions[functionTypeId] = registration;
            return registration;
        }
    }

    internal RAction<TParam, TScrapbook> RegisterMethodAction<TEntity, TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task<Result>>> innerMethodSelector,
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
                    throw new ArgumentException($"{typeof(RAction<TParam, TScrapbook>).SimpleQualifiedName()}> is not compatible with existing {_functions[functionTypeId].GetType().SimpleQualifiedName()}");
                return r;
            }

            var settingsWithDefaults = _settings.Merge(settings);
            if (settingsWithDefaults.DependencyResolver == null)
                throw new ArgumentNullException(nameof(IDependencyResolver), $"Cannot register method when settings' {nameof(IDependencyResolver)} is null");
            
            var commonInvoker = new CommonInvoker(settingsWithDefaults, version, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new RActionMethodInvoker<TEntity, TParam, TScrapbook>(
                functionTypeId, 
                innerMethodSelector, 
                settingsWithDefaults.DependencyResolver,
                new MiddlewarePipeline(settingsWithDefaults.Middlewares),
                concreteScrapbookType,
                commonInvoker,
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
                rFuncInvoker.Invoke,
                rFuncInvoker.ReInvoke,
                rFuncInvoker.ScheduleInvocation,
                rFuncInvoker.ScheduleReInvoke
            );
            _functions[functionTypeId] = registration;
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
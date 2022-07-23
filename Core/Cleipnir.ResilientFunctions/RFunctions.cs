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
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        settings
    );
    
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        settings
    );
    
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Result<TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        settings        
    );
    
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<Result<TReturn>>> inner,
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
            var commonInvoker = new CommonInvoker(settingsWithDefaults, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new RFuncInvoker<TParam, TReturn>(
                functionTypeId, 
                inner, 
                commonInvoker,
                settingsWithDefaults.UnhandledExceptionHandler
            );

            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                reInvoke: (id, statuses, epoch) => rFuncInvoker.ReInvoke(id.ToString(), statuses, epoch),
                settingsWithDefaults,
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
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Action<TParam> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Result> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<Result>> inner,
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
            var commonInvoker = new CommonInvoker(settingsWithDefaults, _functionStore, _shutdownCoordinator);
            var rActionInvoker = new RActionInvoker<TParam>(
                functionTypeId, 
                inner, 
                commonInvoker,
                settingsWithDefaults.UnhandledExceptionHandler
            );

            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                reInvoke: (id, statuses, epoch) => rActionInvoker.ReInvoke(id.ToString(), statuses, epoch),
                settingsWithDefaults,
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
        Settings? settings = null,
        Func<TScrapbook>? scrapbookFactory = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings,
            scrapbookFactory
        );

    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<TReturn>> inner,
        Settings? settings = null,
        Func<TScrapbook>? scrapbookFactory = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings,
            scrapbookFactory
        );

    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Result<TReturn>> inner,
        Settings? settings = null,
        Func<TScrapbook>? scrapbookFactory = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings,
            scrapbookFactory
        );

    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner,
        Settings? settings = null,
        Func<TScrapbook>? scrapbookFactory = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
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
            var commonInvoker = new CommonInvoker(settingsWithDefaults, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new RFuncInvoker<TParam, TScrapbook, TReturn>(
                functionTypeId, 
                inner, 
                scrapbookFactory,
                commonInvoker,
                settingsWithDefaults.UnhandledExceptionHandler
            );

            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                reInvoke: (id, statuses, epoch) => rFuncInvoker.ReInvoke(id.ToString(), statuses, epoch),
                settingsWithDefaults,
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
        Settings? settings = null,
        Func<TScrapbook>? scrapbookFactory = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings,
            scrapbookFactory
        );
    
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Result> inner,
        Settings? settings = null,
        Func<TScrapbook>? scrapbookFactory = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings,
            scrapbookFactory
        );

    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task> inner,
        Settings? settings = null,
        Func<TScrapbook>? scrapbookFactory = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings,
            scrapbookFactory
        );

    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result>> inner,
        Settings? settings = null,
        Func<TScrapbook>? scrapbookFactory = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
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
            var commonInvoker = new CommonInvoker(settingsWithDefaults, _functionStore, _shutdownCoordinator);
            var rActionInvoker = new RActionInvoker<TParam, TScrapbook>(
                functionTypeId, 
                inner, 
                scrapbookFactory,
                commonInvoker,
                settingsWithDefaults.UnhandledExceptionHandler
            );
            
            WatchDogsFactory.CreateAndStart(
                functionTypeId,
                _functionStore,
                (id, statuses, epoch) => rActionInvoker.ReInvoke(id.ToString(), statuses, epoch),
                settingsWithDefaults,
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

    public RJob<TScrapbook> RegisterJob<TScrapbook>(
        string jobId,
        Action<TScrapbook> inner,
        Settings? settings = null
    ) where TScrapbook : RScrapbook, new()
        => RegisterJob(
            jobId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );

    public RJob<TScrapbook> RegisterJob<TScrapbook>(
        string jobId,
        Func<TScrapbook, Result> inner,
        Settings? settings = null
    ) where TScrapbook : RScrapbook, new()
        => RegisterJob(
            jobId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );

    public RJob<TScrapbook> RegisterJob<TScrapbook>(
        string jobId,
        Func<TScrapbook, Task> inner,
        Settings? settings = null
    ) where TScrapbook : RScrapbook, new()
        => RegisterJob(
            jobId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );

    public RJob<TScrapbook> RegisterJob<TScrapbook>(
        string jobId,
        Func<TScrapbook, Task<Result>> inner,
        Settings? settings = null
    ) where TScrapbook : RScrapbook, new()
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
        
        lock (_sync)
        {
            var actionRegistration = RegisterAction(
                jobId,
                RJobExtensions.ToNothingFunc(inner),
                settings
            );
          
            var registration = new RJob<TScrapbook>(
                () => actionRegistration.Schedule("Job", Nothing.Instance),
                (statuses, epoch, scrapbookUpdater) => 
                    actionRegistration.ScheduleReInvocation("Job", statuses, epoch, scrapbookUpdater)
            );

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
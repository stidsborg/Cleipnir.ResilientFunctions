using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.InnerDecorators;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Watchdogs;

namespace Cleipnir.ResilientFunctions;

public class RFunctions : IDisposable 
{
    private readonly Dictionary<FunctionTypeId, object> _functions = new();
    private readonly Dictionary<string, RJob> _jobs = new();

    private readonly IFunctionStore _functionFunctionStore;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly WatchDogsFactory _watchDogsFactory;
    private readonly JobWatchdog _jobWatchdog;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    private readonly ShutdownCoordinator _shutdownCoordinator;
    private volatile bool _disposed;

    private readonly object _sync = new();
    
    public RFunctions(
        IFunctionStore functionStore,
        Action<RFunctionException>? unhandledExceptionHandler = null,
        TimeSpan? crashedCheckFrequency = null,
        TimeSpan? postponedCheckFrequency = null
    )
    {
        crashedCheckFrequency ??= TimeSpan.FromSeconds(10);
        postponedCheckFrequency ??= TimeSpan.FromSeconds(10);
        var exceptionHandler = new UnhandledExceptionHandler(unhandledExceptionHandler ?? (_ => { }));
        var shutdownCoordinator = new ShutdownCoordinator();
            
        var signOfLifeUpdaterFactory = new SignOfLifeUpdaterFactory(
            functionStore,
            exceptionHandler,
            crashedCheckFrequency.Value
        );

        var watchdogsFactory = new WatchDogsFactory(
            functionStore,
            crashedCheckFrequency.Value,
            postponedCheckFrequency.Value,
            exceptionHandler,
            shutdownCoordinator
        );

        _jobWatchdog = new JobWatchdog(
            functionStore,
            postponedCheckFrequency.Value,
            exceptionHandler,
            shutdownCoordinator
        );

        _functionFunctionStore = functionStore;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _watchDogsFactory = watchdogsFactory;
        _unhandledExceptionHandler = exceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }
    
    public Builder.RFunc.BuilderWithInner<TParam, TReturn> Func<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TReturn> inner
    ) where TParam : notnull => new Builder.RFunc.Builder(this, functionTypeId).WithInner(inner);
    
    public Builder.RFunc.BuilderWithInner<TParam, TReturn> Func<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<TReturn>> inner
    ) where TParam : notnull => new Builder.RFunc.Builder(this, functionTypeId).WithInner(inner);
    
    public Builder.RFunc.BuilderWithInner<TParam, TReturn> Func<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Result<TReturn>> inner
    ) where TParam : notnull => new Builder.RFunc.Builder(this, functionTypeId).WithInner(inner);
    
    public Builder.RFunc.BuilderWithInner<TParam, TReturn> Func<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<Result<TReturn>>> inner
    ) where TParam : notnull => new Builder.RFunc.Builder(this, functionTypeId).WithInner(inner);

    internal RFunc<TParam, TReturn> Register<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<Result<TReturn>>> inner,
        Func<Metadata<TParam>, Task>? preInvoke = null,
        Func<Result<TReturn>, Metadata<TParam>, Task<Result<TReturn>>>? postInvoke = null,
        ISerializer? serializer = null
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

            serializer ??= DefaultSerializer.Instance;

            var commonInvoker = new CommonInvoker(
                serializer,
                _functionFunctionStore,
                _shutdownCoordinator,
                _signOfLifeUpdaterFactory
            );
            
            var rFuncInvoker = new RFuncInvoker<TParam, TReturn>(
                functionTypeId, 
                inner, 
                commonInvoker,
                _unhandledExceptionHandler,
                preInvoke ?? Builder.RFunc.CommonAdapters.NoOpPreInvoke<TParam>(),
                postInvoke ?? Builder.RFunc.CommonAdapters.NoOpPostInvoke<TParam, TReturn>()
            );
            
            _watchDogsFactory.CreateAndStart(
                functionTypeId,
                reInvoke: (id, statuses, epoch) => rFuncInvoker.ReInvoke(id.ToString(), statuses, epoch)
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
        ISerializer? serializer = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerDecorators.InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            serializer
        );
    
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Action<TParam> inner,
        ISerializer? serializer = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerDecorators.InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            serializer
        );
    
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Result> inner,
        ISerializer? serializer = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerDecorators.InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            serializer
        );
    
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<Result>> inner,
        ISerializer? serializer = null
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

            var commonInvoker = new CommonInvoker(
                serializer ?? DefaultSerializer.Instance,
                _functionFunctionStore,
                _shutdownCoordinator,
                _signOfLifeUpdaterFactory
            );
            var rActionInvoker = new RActionInvoker<TParam>(
                functionTypeId, 
                inner, 
                commonInvoker,
                _unhandledExceptionHandler
            );

            _watchDogsFactory.CreateAndStart(
                functionTypeId,
                reInvoke: (id, statuses, epoch) => rActionInvoker.ReInvoke(id.ToString(), statuses, epoch)
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

    public Builder.RFunc.BuilderWithInner<TParam, TScrapbook, TReturn> FuncWithScrapbook<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, TReturn> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => new Builder.RFunc.Builder(this, functionTypeId).WithInner(inner);
    
    public Builder.RFunc.BuilderWithInner<TParam, TScrapbook, TReturn> FuncWithScrapbook<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => new Builder.RFunc.Builder(this, functionTypeId).WithInner(inner);

    public Builder.RFunc.BuilderWithInner<TParam, TScrapbook, TReturn> FuncWithScrapbook<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Result<TReturn>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => new Builder.RFunc.Builder(this, functionTypeId).WithInner(inner);
    
    public Builder.RFunc.BuilderWithInner<TParam, TScrapbook, TReturn> FuncWithScrapbook<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => new Builder.RFunc.Builder(this, functionTypeId).WithInner(inner);

    public RFunc<TParam, TReturn> Register<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner,
        Func<TScrapbook, Metadata<TParam>, Task>? preInvoke = null,
        Func<Result<TReturn>, TScrapbook, Metadata<TParam>, Task<Result<TReturn>>>? postInvoke = null,
        ISerializer? serializer = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
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

            serializer ??= DefaultSerializer.Instance;

            var commonInvoker = new CommonInvoker(
                serializer,
                _functionFunctionStore,
                _shutdownCoordinator,
                _signOfLifeUpdaterFactory
            );
            var rFuncInvoker = new RFuncInvoker<TParam, TScrapbook, TReturn>(
                functionTypeId, 
                inner, 
                commonInvoker,
                _unhandledExceptionHandler,
                preInvoke ?? Builder.RFunc.CommonAdapters.NoOpPreInvoke<TParam, TScrapbook>(),
                postInvoke ?? Builder.RFunc.CommonAdapters.NoOpPostInvoke<TParam, TScrapbook, TReturn>()
            );

            _watchDogsFactory.CreateAndStart(
                functionTypeId,
                reInvoke: (id, statuses, epoch) => rFuncInvoker.ReInvoke(id.ToString(), statuses, epoch)
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

    public RAction<TParam> RegisterActionWithScrapbook<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Action<TParam, TScrapbook> inner,
        ISerializer? serializer = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterActionWithScrapbook(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            serializer
        );
    
    public RAction<TParam> RegisterActionWithScrapbook<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Result> inner,
        ISerializer? serializer = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterActionWithScrapbook(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            serializer
        );

    public RAction<TParam> RegisterActionWithScrapbook<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task> inner,
        ISerializer? serializer = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterActionWithScrapbook(
            functionTypeId,
            InnerToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            serializer
        );

    public RAction<TParam> RegisterActionWithScrapbook<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result>> inner,
        ISerializer? serializer = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
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
            
            serializer ??= DefaultSerializer.Instance;

            var commonInvoker = new CommonInvoker(
                serializer,
                _functionFunctionStore,
                _shutdownCoordinator,
                _signOfLifeUpdaterFactory
            );
            var rActionInvoker = new RActionInvoker<TParam, TScrapbook>(
                functionTypeId, 
                inner, 
                commonInvoker,
                _unhandledExceptionHandler
            );
            
            _watchDogsFactory.CreateAndStart(
                functionTypeId,
                (id, statuses, epoch) => rActionInvoker.ReInvoke(id.ToString(), statuses, epoch)
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

    public Builder.RJob.Builder<TScrapbook> RegisterJob<TScrapbook>(string jobId) 
        where TScrapbook : RScrapbook, new() => new(this, jobId);

    internal RJob RegisterJob<TScrapbook>(
        string jobId,
        Func<TScrapbook, Task<Result>> inner,
        Func<TScrapbook, Task>? preInvoke = null,
        Func<Result, TScrapbook, Task<Result>>? postInvoke = null,
        ISerializer? serializer = null
    ) where TScrapbook : RScrapbook, new()
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
        
        lock (_sync)
        {
            if (_jobs.ContainsKey(jobId))
                return _jobs[jobId];
            
            serializer ??= DefaultSerializer.Instance;

            var commonInvoker = new CommonInvoker(
                serializer,
                _functionFunctionStore,
                _shutdownCoordinator,
                _signOfLifeUpdaterFactory
            );

            var rJobInvoker = new RJobInvoker<TScrapbook>(
                jobId,
                inner,
                preInvoke,
                postInvoke,
                commonInvoker,
                _unhandledExceptionHandler
            );

            _jobWatchdog.AddJob(
                jobId,
                (_, statuses, epoch) => rJobInvoker.ForceContinuation(statuses, epoch)
            );

            var registration = new RJob(
                rJobInvoker.Start,
                rJobInvoker.ForceContinuation
            );

            _jobs[jobId] = registration;
            
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
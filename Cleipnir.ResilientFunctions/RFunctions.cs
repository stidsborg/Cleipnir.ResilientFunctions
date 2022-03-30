using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
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

    private readonly IFunctionStore _functionFunctionStore;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly WatchDogsFactory _watchDogsFactory;
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

        _functionFunctionStore = functionStore;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _watchDogsFactory = watchdogsFactory;
        _unhandledExceptionHandler = exceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public RFunc<TParam, TReturn> Register<TParam, TReturn>(
        FunctionTypeId functionTypeId, InnerFunc<TParam, TReturn> inner
    ) where TParam : notnull 
        => Register(functionTypeId, inner, preInvoke: null, postInvoke: null, serializer: null);
    
    public RFunc<TParam, TReturn> Register<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        InnerFunc<TParam, TReturn> inner,
        ISerializer? serializer
    ) where TParam : notnull
        => Register(functionTypeId, inner, preInvoke: null, postInvoke: null, serializer);

    public RFunc<TParam, TReturn> Register<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        InnerFunc<TParam, TReturn> inner,
        RFunc.SyncPreInvoke<TParam>? preInvoke,
        RFunc.SyncPostInvoke<TParam, TReturn>? postInvoke,
        ISerializer? serializer = null
    ) where TParam : notnull => Register(
        functionTypeId,
        inner,
        CommonInvoker.SyncedFuncPreInvoke(preInvoke),
        CommonInvoker.SyncedFuncPostInvoke(postInvoke),
        serializer
    );
    
    public RFunc<TParam, TReturn> Register<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        InnerFunc<TParam, TReturn> inner,
        RFunc.PreInvoke<TParam>? preInvoke,
        RFunc.PostInvoke<TParam, TReturn>? postInvoke,
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
                preInvoke,
                postInvoke
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

    public RAction<TParam> Register<TParam>(FunctionTypeId functionTypeId, InnerAction<TParam> inner) 
        where TParam : notnull
        => Register(functionTypeId, inner, preInvoke: null, postInvoke: null, serializer: null);
    
    public RAction<TParam> Register<TParam>(
        FunctionTypeId functionTypeId,
        InnerAction<TParam> inner,
        ISerializer? serializer
    ) where TParam : notnull
        => Register(functionTypeId, inner, preInvoke: null, postInvoke: null, serializer);

    public RAction<TParam> Register<TParam>(
        FunctionTypeId functionTypeId,
        InnerAction<TParam> inner,
        RAction.SyncPreInvoke<TParam>? preInvoke,
        RAction.SyncPostInvoke<TParam>? postInvoke,
        ISerializer? serializer = null
    ) where TParam : notnull
        => Register(
            functionTypeId,
            inner,
            CommonInvoker.SyncedActionPreInvoke(preInvoke),
            CommonInvoker.SyncedActionPostInvoke(postInvoke),
            serializer
        );
        
    public RAction<TParam> Register<TParam>(
        FunctionTypeId functionTypeId,
        InnerAction<TParam> inner,
        RAction.PreInvoke<TParam>? preInvoke,
        RAction.PostInvoke<TParam>? postInvoke,
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
            serializer ??= DefaultSerializer.Instance;

            var commonInvoker = new CommonInvoker(
                serializer,
                _functionFunctionStore,
                _shutdownCoordinator,
                _signOfLifeUpdaterFactory
            );
            var rActionInvoker = new RActionInvoker<TParam>(
                functionTypeId, 
                inner, 
                commonInvoker,
                _unhandledExceptionHandler,
                preInvoke,
                postInvoke
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

    public RFunc<TParam, TReturn> Register<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId, InnerFunc<TParam, TScrapbook, TReturn> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => Register(functionTypeId, inner, preInvoke: null, postInvoke: null, serializer: null);
    
    public RFunc<TParam, TReturn> Register<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        InnerFunc<TParam, TScrapbook, TReturn> inner,
        ISerializer? serializer
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => Register(functionTypeId, inner, preInvoke: null, postInvoke: null, serializer);
    
    public RFunc<TParam, TReturn> Register<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        InnerFunc<TParam, TScrapbook, TReturn> inner,
        RFunc.SyncPreInvoke<TParam, TScrapbook>? preInvoke,
        RFunc.SyncPostInvoke<TParam, TScrapbook, TReturn>? postInvoke,
        ISerializer? serializer = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => Register(
            functionTypeId,
            inner,
            CommonInvoker.SyncedFuncPreInvoke(preInvoke),
            CommonInvoker.SyncedFuncPostInvoke(postInvoke),
            serializer
        );
    
    public RFunc<TParam, TReturn> Register<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        InnerFunc<TParam, TScrapbook, TReturn> inner,
        RFunc.PreInvoke<TParam, TScrapbook>? preInvoke,
        RFunc.PostInvoke<TParam, TScrapbook, TReturn>? postInvoke,
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
                preInvoke,
                postInvoke
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
    
    public RAction<TParam> Register<TParam, TScrapbook>(
        FunctionTypeId functionTypeId, InnerAction<TParam, TScrapbook> inner
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => Register(functionTypeId, inner, preInvoke: null, postInvoke: null, serializer: null);
    
    public RAction<TParam> Register<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        InnerAction<TParam, TScrapbook> inner,
        ISerializer? serializer
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => Register(functionTypeId, inner, preInvoke: null, postInvoke: null, serializer);

    public RAction<TParam> Register<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        InnerAction<TParam, TScrapbook> inner,
        RAction.SyncPreInvoke<TParam, TScrapbook>? preInvoke,
        RAction.SyncPostInvoke<TParam, TScrapbook>? postInvoke,
        ISerializer? serializer = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => Register(
            functionTypeId,
            inner,
            CommonInvoker.SyncedActionPreInvoke(preInvoke),
            CommonInvoker.SyncedActionPostInvoke(postInvoke),
            serializer
        );
    
    public RAction<TParam> Register<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        InnerAction<TParam, TScrapbook> inner,
        RAction.PreInvoke<TParam, TScrapbook>? preInvoke,
        RAction.PostInvoke<TParam, TScrapbook>? postInvoke,
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
                _unhandledExceptionHandler, 
                preInvoke,
                postInvoke
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
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
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

    private readonly IFunctionStore _functionStore;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly WatchDogsFactory _watchDogsFactory;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    private readonly ShutdownCoordinator _shutdownCoordinator;
    private volatile bool _disposed;

    private readonly object _sync = new();
    
    public RFunctions(
        IFunctionStore store,
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
            store,
            exceptionHandler,
            crashedCheckFrequency.Value
        );

        var watchdogsFactory = new WatchDogsFactory(
            store,
            signOfLifeUpdaterFactory,
            crashedCheckFrequency.Value,
            postponedCheckFrequency.Value,
            exceptionHandler,
            shutdownCoordinator
        );

        _functionStore = store;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _watchDogsFactory = watchdogsFactory;
        _unhandledExceptionHandler = exceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public RFunc<TParam, TReturn> Register<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        InnerFunc<TParam, TReturn> inner,
        ISerializer? serializer = null,
        OnFuncException<TParam, TReturn>? onException = null
    ) where TParam : notnull
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
            
        lock (_sync)
        {
            //todo consider throwing exception if the method is not equal to the previously registered one...?!
            if (_functions.ContainsKey(functionTypeId))
                return (RFunc<TParam, TReturn>) _functions[functionTypeId];

            serializer ??= DefaultSerializer.Instance;
                
            _watchDogsFactory.CreateAndStart(
                functionTypeId,
                serializer,
                (param, _) => inner((TParam) param)
            );

            var commonInvoker = new CommonInvoker(serializer, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new RFuncInvoker<TParam, TReturn>(
                functionTypeId, 
                inner, 
                commonInvoker,
                _signOfLifeUpdaterFactory, 
                _shutdownCoordinator,
                _unhandledExceptionHandler,
                onException
            );

            var registration = new RFunc<TParam, TReturn>(
                rFuncInvoker.Invoke,
                rFuncInvoker.ReInvoke,
                rFuncInvoker.ScheduleInvocation
            );
            _functions[functionTypeId] = registration;
            return registration;
        }
    }
        
    public RAction<TParam> Register<TParam>(
        FunctionTypeId functionTypeId,
        InnerAction<TParam> inner,
        ISerializer? serializer = null,
        OnActionException<TParam>? onException = null
    ) where TParam : notnull 
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
            
        lock (_sync)
        {
            //todo consider throwing exception if the method is not equal to the previously registered one...?!
            if (_functions.ContainsKey(functionTypeId))
                return (RAction<TParam>) _functions[functionTypeId];

            serializer ??= DefaultSerializer.Instance;
                
            _watchDogsFactory.CreateAndStart(
                functionTypeId,
                serializer,
                (param, _) => inner((TParam) param)
            );

            var commonInvoker = new CommonInvoker(serializer, _functionStore, _shutdownCoordinator);
            var rActionInvoker = new RActionInvoker<TParam>(
                functionTypeId, 
                inner, 
                commonInvoker,
                _signOfLifeUpdaterFactory,
                _shutdownCoordinator,
                _unhandledExceptionHandler, 
                onException
            );
                
            var registration =  new RAction<TParam>(
                rActionInvoker.Invoke,
                rActionInvoker.ReInvoke,
                rActionInvoker.ScheduleInvocation
            );
            _functions[functionTypeId] = registration;
            return registration;
        }
    }

    public RFunc<TParam, TReturn> Register<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        InnerFunc<TParam, TScrapbook, TReturn> inner,
        ISerializer? serializer = null,
        OnFuncException<TParam, TScrapbook, TReturn>? onException = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
            
        lock (_sync)
        {
            //todo consider throwing exception if the method is not equal to the previously registered one...?!
            if (_functions.ContainsKey(functionTypeId))
                return (RFunc<TParam, TReturn>) _functions[functionTypeId];

            serializer ??= DefaultSerializer.Instance;
                
            _watchDogsFactory.CreateAndStart(
                functionTypeId,
                serializer,
                (param, scrapbook) => inner((TParam) param, (TScrapbook) scrapbook!)
            );

            var commonInvoker = new CommonInvoker(serializer, _functionStore, _shutdownCoordinator);
            var rFuncInvoker = new RFuncInvoker<TParam, TScrapbook, TReturn>(
                functionTypeId, 
                inner, 
                commonInvoker,
                _signOfLifeUpdaterFactory, 
                _shutdownCoordinator,
                _unhandledExceptionHandler,
                onException
            );
                
            var registration = new RFunc<TParam, TReturn>(
                rFuncInvoker.Invoke,
                rFuncInvoker.ReInvoke,
                rFuncInvoker.ScheduleInvocation
            );
            _functions[functionTypeId] = registration;
            return registration;
        }
    }
        
    public RAction<TParam> Register<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        InnerAction<TParam, TScrapbook> inner,
        ISerializer? serializer = null,
        OnActionException<TParam, TScrapbook>? onException = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
            
        lock (_sync)
        {
            //todo consider throwing exception if the method is not equal to the previously registered one...?!
            if (_functions.ContainsKey(functionTypeId))
                return (RAction<TParam>) _functions[functionTypeId];
                

            serializer ??= DefaultSerializer.Instance;
                
            _watchDogsFactory.CreateAndStart(
                functionTypeId,
                serializer,
                (param, scrapbook) => inner((TParam) param, (TScrapbook) scrapbook!)
            );

            var commonInvoker = new CommonInvoker(serializer, _functionStore, _shutdownCoordinator);
            var rActionInvoker = new RActionInvoker<TParam, TScrapbook>(
                functionTypeId, 
                inner, 
                commonInvoker,
                _signOfLifeUpdaterFactory, 
                _shutdownCoordinator,
                _unhandledExceptionHandler, 
                onException
            );
                
            var registration = new RAction<TParam>(
                rActionInvoker.Invoke,
                rActionInvoker.ReInvoke,
                rActionInvoker.ScheduleInvocation
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

    public static RFunctions Create(
        IFunctionStore store,
        Action<RFunctionException>? unhandledExceptionHandler = null,
        TimeSpan? crashedCheckFrequency = null,
        TimeSpan? postponedCheckFrequency = null
    ) => new RFunctions(store, unhandledExceptionHandler, crashedCheckFrequency, postponedCheckFrequency);
}
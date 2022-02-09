using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Watchdogs;

namespace Cleipnir.ResilientFunctions
{
    public class RFunctions : IDisposable 
    {
        private readonly Dictionary<FunctionTypeId, Delegate> _functions = new();

        private readonly IFunctionStore _functionStore;
        private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
        private readonly WatchDogsFactory _watchDogsFactory;
        private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

        private readonly ShutdownCoordinator _shutdownCoordinator;
        private volatile bool _disposed;

        private readonly object _sync = new();

        private RFunctions(
            IFunctionStore functionStore, 
            SignOfLifeUpdaterFactory signOfLifeUpdaterFactory,
            WatchDogsFactory watchDogsFactory, 
            UnhandledExceptionHandler unhandledExceptionHandler,
            ShutdownCoordinator shutdownCoordinator
        )
        {
            _functionStore = functionStore;
            _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
            _watchDogsFactory = watchDogsFactory;
            _unhandledExceptionHandler = unhandledExceptionHandler;
            _shutdownCoordinator = shutdownCoordinator;
        }
        
        public RFunc<TParam, TReturn> Register<TParam, TReturn>(
            FunctionTypeId functionTypeId,
            Func<TParam, Task<RResult<TReturn>>> func,
            Func<TParam, object> idFunc,
            ISerializer? serializer = null
        ) where TParam : notnull where TReturn : notnull
        {
            if (_disposed)
                throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
            
            lock (_sync)
            {
                //todo consider throwing exception if the method is not equal to the previously registered one...?!
                if (_functions.ContainsKey(functionTypeId))
                    return (RFunc<TParam, TReturn>) _functions[functionTypeId];

                serializer ??= new DefaultSerializer();
                
                _watchDogsFactory.CreateAndStart(
                    functionTypeId,
                    serializer,
                    (param, _) => func((TParam) param)
                );

                var rFuncInvoker = new RFuncInvoker<TParam, TReturn>(
                    functionTypeId, idFunc, func, 
                    _functionStore, 
                    serializer,
                    _signOfLifeUpdaterFactory, 
                    _unhandledExceptionHandler,
                    _shutdownCoordinator
                );

                var rFunc = new RFunc<TParam, TReturn>(rFuncInvoker.Invoke);
                _functions[functionTypeId] = rFunc;
                return rFunc;
            }
        }
        
        public RActionRegistration<TParam> Register<TParam>(
            FunctionTypeId functionTypeId,
            Func<TParam, Task<RResult>> func,
            Func<TParam, object> idFunc,
            ISerializer? serializer = null
        ) where TParam : notnull 
        {
            if (_disposed)
                throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
            
            lock (_sync)
            {
                //todo consider throwing exception if the method is not equal to the previously registered one...?!
                if (_functions.ContainsKey(functionTypeId))
                    return new RActionRegistration<TParam>(
                        (RAction<TParam>) _functions[functionTypeId],
                        (_, _, _) => throw new NotImplementedException(),
                        id => throw new NotImplementedException()
                    );

                serializer ??= new DefaultSerializer();
                
                _watchDogsFactory.CreateAndStart(
                    functionTypeId,
                    serializer,
                    (param, _) => func((TParam) param)
                );

                var rActionInvoker = new RActionInvoker<TParam>(
                    functionTypeId, idFunc, func, 
                    _functionStore, 
                    serializer,
                    _signOfLifeUpdaterFactory, 
                    _unhandledExceptionHandler,
                    _shutdownCoordinator
                );

                var rAction = new RAction<TParam>(rActionInvoker.Invoke);
                _functions[functionTypeId] = rAction;
                return new RActionRegistration<TParam>(
                    rAction,
                    (_, _, _) => throw new NotImplementedException(),
                    id => throw new NotImplementedException()
                );
            }
        }

        public RFunc<TParam, TReturn> Register<TParam, TScrapbook, TReturn>(
            FunctionTypeId functionTypeId,
            Func<TParam, TScrapbook, Task<RResult<TReturn>>> func,
            Func<TParam, object> idFunc,
            ISerializer? serializer = null
        ) where TParam : notnull where TScrapbook : RScrapbook, new() where TReturn : notnull
        {
            if (_disposed)
                throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
            
            lock (_sync)
            {
                //todo consider throwing exception if the method is not equal to the previously registered one...?!
                if (_functions.ContainsKey(functionTypeId))
                    return (RFunc<TParam, TReturn>) _functions[functionTypeId];

                serializer ??= new DefaultSerializer();
                
                _watchDogsFactory.CreateAndStart(
                    functionTypeId,
                    serializer,
                    (param, scrapbook) => func((TParam) param, (TScrapbook) scrapbook!)
                );

                var rFuncInvoker = new RFuncInvoker<TParam, TScrapbook, TReturn>(
                    functionTypeId, idFunc, func, 
                    _functionStore, 
                    serializer,
                    _signOfLifeUpdaterFactory, 
                    _unhandledExceptionHandler,
                    _shutdownCoordinator
                );

                var rFunc = new RFunc<TParam, TReturn>(rFuncInvoker.Invoke);
                _functions[functionTypeId] = rFunc;
                return rFunc;
            }
        }
        
        public RActionRegistration<TParam, TScrapbook> Register<TParam, TScrapbook>(
            FunctionTypeId functionTypeId,
            Func<TParam, TScrapbook, Task<RResult>> func,
            Func<TParam, object> idFunc,
            ISerializer? serializer = null
        ) where TParam : notnull where TScrapbook : RScrapbook, new()
        {
            if (_disposed)
                throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
            
            lock (_sync)
            {
                //todo consider throwing exception if the method is not equal to the previously registered one...?!
                if (_functions.ContainsKey(functionTypeId))
                    return new RActionRegistration<TParam, TScrapbook>(
                        (RAction<TParam>) _functions[functionTypeId],
                        (_, _, _) => throw new NotImplementedException(),
                        id => throw new NotImplementedException()
                    );
                

                serializer ??= new DefaultSerializer();
                
                _watchDogsFactory.CreateAndStart(
                    functionTypeId,
                    serializer,
                    (param, scrapbook) => func((TParam) param, (TScrapbook) scrapbook!)
                );

                var rActionInvoker = new RActionInvoker<TParam, TScrapbook>(
                    functionTypeId, idFunc, func, 
                    _functionStore, 
                    serializer,
                    _signOfLifeUpdaterFactory, 
                    _unhandledExceptionHandler,
                    _shutdownCoordinator
                );

                var rAction = new RAction<TParam>(rActionInvoker.Invoke);
                _functions[functionTypeId] = rAction;
                return new RActionRegistration<TParam, TScrapbook>(
                    rAction,
                    (_, _, _) => throw new NotImplementedException(),
                    id => throw new NotImplementedException()
                );
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
            
            var rFunctions = new RFunctions(
                store,
                signOfLifeUpdaterFactory,
                watchdogsFactory, 
                exceptionHandler,
                shutdownCoordinator
            );
            
            return rFunctions;
        }
    }
}
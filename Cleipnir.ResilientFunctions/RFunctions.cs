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
        private readonly Dictionary<FunctionTypeId, object> _functions = new();

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
        
        public RFuncRegistration<TParam, TReturn> Register<TParam, TReturn>(
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
                    return (RFuncRegistration<TParam, TReturn>) _functions[functionTypeId];

                serializer ??= new DefaultSerializer();
                
                _watchDogsFactory.CreateAndStart(
                    functionTypeId,
                    serializer,
                    (param, _) => func((TParam) param)
                );

                var commonInvoker = new CommonInvoker(
                    serializer,
                    _functionStore,
                    _unhandledExceptionHandler,
                    _shutdownCoordinator
                );
                var rFuncInvoker = new RFuncInvoker<TParam, TReturn>(
                    functionTypeId, idFunc, func, 
                    commonInvoker,
                    _signOfLifeUpdaterFactory, 
                    _shutdownCoordinator,
                    _unhandledExceptionHandler
                );

                var registration = new RFuncRegistration<TParam, TReturn>(
                    rFuncInvoker.Invoke,
                    (_, _, _) => throw new NotImplementedException(),
                    rFuncInvoker.ScheduleInvocation
                );
                _functions[functionTypeId] = registration;
                return registration;
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
                    return (RActionRegistration<TParam>) _functions[functionTypeId];

                serializer ??= new DefaultSerializer();
                
                _watchDogsFactory.CreateAndStart(
                    functionTypeId,
                    serializer,
                    (param, _) => func((TParam) param)
                );

                var commonInvoker = new CommonInvoker(
                    serializer, _functionStore, _unhandledExceptionHandler, _shutdownCoordinator
                );
                var rActionInvoker = new RActionInvoker<TParam>(
                    functionTypeId, idFunc, func, 
                    commonInvoker,
                    _signOfLifeUpdaterFactory,
                    _shutdownCoordinator,
                    _unhandledExceptionHandler
                );
                
                var registration =  new RActionRegistration<TParam>(
                    rActionInvoker.Invoke,
                    (_, _, _) => throw new NotImplementedException(),
                    rActionInvoker.ScheduleInvocation
                );
                _functions[functionTypeId] = registration;
                return registration;
            }
        }

        public RFuncRegistration<TParam, TScrapbook, TReturn> Register<TParam, TScrapbook, TReturn>(
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
                    return (RFuncRegistration<TParam, TScrapbook, TReturn>) _functions[functionTypeId];

                serializer ??= new DefaultSerializer();
                
                _watchDogsFactory.CreateAndStart(
                    functionTypeId,
                    serializer,
                    (param, scrapbook) => func((TParam) param, (TScrapbook) scrapbook!)
                );

                var commonInvoker = new CommonInvoker(
                    serializer,
                    _functionStore,
                    _unhandledExceptionHandler,
                    _shutdownCoordinator
                );
                var rFuncInvoker = new RFuncInvoker<TParam, TScrapbook, TReturn>(
                    functionTypeId, idFunc, func, 
                    commonInvoker,
                    _signOfLifeUpdaterFactory, 
                    _shutdownCoordinator,
                    _unhandledExceptionHandler
                );
                
                var registration = new RFuncRegistration<TParam, TScrapbook, TReturn>(
                    rFuncInvoker.Invoke,
                    (_, _, _) => throw new NotImplementedException(),
                    rFuncInvoker.ScheduleInvocation
                );
                _functions[functionTypeId] = registration;
                return registration;
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
                    return (RActionRegistration<TParam, TScrapbook>) _functions[functionTypeId];
                

                serializer ??= new DefaultSerializer();
                
                _watchDogsFactory.CreateAndStart(
                    functionTypeId,
                    serializer,
                    (param, scrapbook) => func((TParam) param, (TScrapbook) scrapbook!)
                );

                var commonInvoker = new CommonInvoker(
                    serializer, _functionStore, _unhandledExceptionHandler, _shutdownCoordinator
                );
                var rActionInvoker = new RActionInvoker<TParam, TScrapbook>(
                    functionTypeId, idFunc, func, 
                    commonInvoker,
                    _signOfLifeUpdaterFactory, 
                    _shutdownCoordinator,
                    _unhandledExceptionHandler
                );
                
                var registration = new RActionRegistration<TParam, TScrapbook>(
                    rActionInvoker.Invoke,
                    (_, _, _) => throw new NotImplementedException(),
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
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Watchdogs;
using Cleipnir.ResilientFunctions.Watchdogs.Invocation;

namespace Cleipnir.ResilientFunctions
{
    public class RFunctions : IDisposable
    {
        private readonly Dictionary<FunctionTypeId, Delegate> _functions = new();
        private readonly List<IDisposable> _watchDogs = new();

        private readonly IFunctionStore _functionStore;
        private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
        private readonly WatchDogsFactory _watchDogsFactory;
        private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

        private readonly object _sync = new();

        private RFunctions(
            IFunctionStore functionStore, 
            SignOfLifeUpdaterFactory signOfLifeUpdaterFactory,
            WatchDogsFactory watchDogsFactory, 
            UnhandledExceptionHandler unhandledExceptionHandler)
        {
            _functionStore = functionStore;
            _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
            _watchDogsFactory = watchDogsFactory;
            _unhandledExceptionHandler = unhandledExceptionHandler;
        }
        
        public RFunc<TParam, TReturn> Register<TParam, TReturn>(
            FunctionTypeId functionTypeId,
            Func<TParam, Task<RResult<TReturn>>> func,
            Func<TParam, object> idFunc
        ) where TParam : notnull where TReturn : notnull
        {
            lock (_sync)
            {
                //todo consider throwing exception if the method is not equal to the previously registered one...?!
                if (_functions.ContainsKey(functionTypeId))
                    return (RFunc<TParam, TReturn>) _functions[functionTypeId];

                var (crashedWatchdog, postponedWatchdog) = _watchDogsFactory.CreateAndStart(
                    functionTypeId,
                    (param, _) => func((TParam) param)
                );

                _watchDogs.AddRange(new IDisposable[] { crashedWatchdog, postponedWatchdog });

                var rFuncInvoker = new RFuncInvoker<TParam, TReturn>(
                    functionTypeId, idFunc, func, _functionStore, _signOfLifeUpdaterFactory, _unhandledExceptionHandler
                );

                var rFunc = new RFunc<TParam, TReturn>(rFuncInvoker.Invoke);
                _functions[functionTypeId] = rFunc;
                return rFunc;
            }
        }
        
        public RAction<TParam> Register<TParam>(
            FunctionTypeId functionTypeId,
            Func<TParam, Task<RResult>> func,
            Func<TParam, object> idFunc
        ) where TParam : notnull 
        {
            lock (_sync)
            {
                //todo consider throwing exception if the method is not equal to the previously registered one...?!
                if (_functions.ContainsKey(functionTypeId))
                    return (RAction<TParam>) _functions[functionTypeId];

                var (crashedWatchdog, postponedWatchdog) = _watchDogsFactory.CreateAndStart(
                    functionTypeId,
                    (param, _) => func((TParam) param)
                );

                _watchDogs.AddRange(new IDisposable[] { crashedWatchdog, postponedWatchdog });

                var rActionInvoker = new RActionInvoker<TParam>(
                    functionTypeId, idFunc, func, _functionStore, _signOfLifeUpdaterFactory, _unhandledExceptionHandler
                );

                var rAction = new RAction<TParam>(rActionInvoker.Invoke);
                _functions[functionTypeId] = rAction;
                return rAction;
            }
        }

        public RFunc<TParam, TReturn> Register<TParam, TScrapbook, TReturn>(
            FunctionTypeId functionTypeId,
            Func<TParam, TScrapbook, Task<RResult<TReturn>>> func,
            Func<TParam, object> idFunc) 
            where TParam : notnull 
            where TScrapbook : RScrapbook, new()
            where TReturn : notnull
        {
            lock (_sync)
            {
                //todo consider throwing exception if the method is not equal to the previously registered one...?!
                if (_functions.ContainsKey(functionTypeId))
                    return (RFunc<TParam, TReturn>) _functions[functionTypeId];

                var (crashedWatchdog, postponedWatchdog) = _watchDogsFactory.CreateAndStart(
                    functionTypeId,
                    (param, scrapbook) => func((TParam) param, (TScrapbook) scrapbook!)
                );

                _watchDogs.AddRange(new IDisposable[] { crashedWatchdog, postponedWatchdog });

                var rFuncInvoker = new RFuncInvoker<TParam, TScrapbook, TReturn>(
                    functionTypeId, idFunc, func, _functionStore, _signOfLifeUpdaterFactory, _unhandledExceptionHandler
                );

                var rFunc = new RFunc<TParam, TReturn>(rFuncInvoker.Invoke);
                _functions[functionTypeId] = rFunc;
                return rFunc;
            }
        }
        
        public RAction<TParam> Register<TParam, TScrapbook>(
            FunctionTypeId functionTypeId,
            Func<TParam, TScrapbook, Task<RResult>> func,
            Func<TParam, object> idFunc
        ) where TParam : notnull where TScrapbook : RScrapbook, new()
        {
            lock (_sync)
            {
                //todo consider throwing exception if the method is not equal to the previously registered one...?!
                if (_functions.ContainsKey(functionTypeId))
                    return (RAction<TParam>) _functions[functionTypeId];

                var (crashedWatchdog, postponedWatchdog) = _watchDogsFactory.CreateAndStart(
                    functionTypeId,
                    (param, scrapbook) => func((TParam) param, (TScrapbook) scrapbook!)
                );

                _watchDogs.AddRange(new IDisposable[] { crashedWatchdog, postponedWatchdog });

                var rActionInvoker = new RActionInvoker<TParam, TScrapbook>(
                    functionTypeId, idFunc, func, _functionStore, _signOfLifeUpdaterFactory, _unhandledExceptionHandler
                );

                var rAction = new RAction<TParam>(rActionInvoker.Invoke);
                _functions[functionTypeId] = rAction;
                return rAction;
            }
        }

        public void Dispose()
        {
            lock (_sync)
            {
                foreach (var unhandledWatchDog in _watchDogs)
                    unhandledWatchDog.Dispose();

                _watchDogs.Clear();
            }
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

            var signOfLifeUpdaterFactory = new SignOfLifeUpdaterFactory(
                store,
                exceptionHandler,
                crashedCheckFrequency.Value
            );
            var functionRetryer = new RFuncInvoker(store, signOfLifeUpdaterFactory, exceptionHandler);

            var watchdogsFactory = new WatchDogsFactory(
                store,
                functionRetryer,
                crashedCheckFrequency.Value,
                postponedCheckFrequency.Value,
                exceptionHandler
            );
            
            var rFunctions = new RFunctions(
                store,
                signOfLifeUpdaterFactory,
                watchdogsFactory, 
                exceptionHandler
            );
            
            return rFunctions;
        } 
    }
}
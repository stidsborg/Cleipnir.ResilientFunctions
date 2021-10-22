using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Utils;

namespace Cleipnir.ResilientFunctions
{
    public class RFunctions : IDisposable
    {
        private readonly Dictionary<FunctionTypeId, Delegate> _functions = new();
        private readonly List<IDisposable> _unhandledWatchDogs = new();

        private readonly IFunctionStore _functionStore;
        private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;

        private readonly TimeSpan _unhandledWatchDogCheckFrequency;
        private readonly Action<RFunctionException> _unhandledExceptionHandler;

        private readonly object _sync = new();

        private RFunctions(
            IFunctionStore functionStore, 
            SignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
            TimeSpan unhandledWatchDogCheckFrequency, 
            Action<RFunctionException> unhandledExceptionHandler)
        {
            _functionStore = functionStore;
            _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
            _unhandledWatchDogCheckFrequency = unhandledWatchDogCheckFrequency;
            _unhandledExceptionHandler = unhandledExceptionHandler;
        }

        public RFunction<TParam, TReturn> Register<TParam, TReturn>(
            FunctionTypeId functionTypeId, 
            TParam? paramExample, 
            Func<TParam, Task<TReturn>> func
        ) where TParam : notnull where TReturn : notnull
        {
            _ = paramExample;
            
            lock (_sync)
            {
                if (_functions.ContainsKey(functionTypeId))
                    return (RFunction<TParam, TReturn>) _functions[functionTypeId];

                Task<TReturn> RFunc(TParam param, FunctionInstanceId? id = null)
                {
                    if (id == null)
                    {
                        var paramJson = JsonSerializer.Serialize(param);
                        var paramHash = HashHelper.SHA256Hash(paramJson);
                        id = paramHash.ToFunctionInstanceId();    
                    }
                    
                    var runner = new RFunctionRunner<TParam, TReturn>(
                        new FunctionId(functionTypeId, id),
                        _functionStore,
                        func,
                        param,
                        _signOfLifeUpdaterFactory
                    );
                    
                    return runner.InvokeMethodAndStoreResult();
                }

                var watchdog = new UnhandledRFunctionWatchdog<TParam, TReturn>(
                    functionTypeId,
                    func,
                    _functionStore,
                    _signOfLifeUpdaterFactory,
                    _unhandledWatchDogCheckFrequency,
                    _unhandledExceptionHandler
                );
                _ = watchdog.Start();

                _unhandledWatchDogs.Add(watchdog);

                _functions[functionTypeId] = new RFunction<TParam, TReturn>(RFunc);
                
                return RFunc;
            }
        }
        
        public void Dispose()
        {
            lock (_sync)
            {
                foreach (var unhandledWatchDog in _unhandledWatchDogs)
                    unhandledWatchDog.Dispose();

                _unhandledWatchDogs.Clear();
            }
        }

        public static RFunctions Create(
            IFunctionStore store,
            Action<RFunctionException>? unhandledExceptionHandler = null,
            TimeSpan? unhandledFunctionsCheckFrequency = null) 
            => new RFunctions(
                store,
                new SignOfLifeUpdaterFactory(
                    store,
                    unhandledExceptionHandler ?? (_ => {}),
                    unhandledFunctionsCheckFrequency ?? TimeSpan.FromSeconds(10)
                ),
                unhandledFunctionsCheckFrequency ?? TimeSpan.FromSeconds(10),
                unhandledExceptionHandler ?? (_ => {})
            );
    }
}
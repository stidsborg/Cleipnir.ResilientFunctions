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

        private readonly object _sync = new();

        public RFunctions(
            IFunctionStore functionStore, 
            SignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
            TimeSpan unhandledWatchDogCheckFrequency 
        )
        {
            _functionStore = functionStore;
            _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
            _unhandledWatchDogCheckFrequency = unhandledWatchDogCheckFrequency;
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

                var watchdog = new UnhandledWatchdog<TParam, TReturn>(
                    functionTypeId,
                    func,
                    _functionStore,
                    _signOfLifeUpdaterFactory,
                    _unhandledWatchDogCheckFrequency
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

        public static RFunctions Create(IFunctionStore store, TimeSpan? unhandledFunctionsCheckFrequency = null) 
            => new RFunctions(
                store,
                new SignOfLifeUpdaterFactory(
                    store,
                    unhandledFunctionsCheckFrequency ?? TimeSpan.FromSeconds(10)
                ),
                unhandledFunctionsCheckFrequency ?? TimeSpan.FromSeconds(10)
            );
    }
}
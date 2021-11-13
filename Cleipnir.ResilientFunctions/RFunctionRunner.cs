using System;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Utils;

namespace Cleipnir.ResilientFunctions
{
    internal class RFunctionRunner<TParam, TReturn> where TParam : notnull where TReturn : notnull
    {
        private readonly FunctionId _functionId;
        private readonly Func<TParam, Task<TReturn>> _func;
        private readonly TParam _param;

        private readonly IFunctionStore _functionStore;
        private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;

        public RFunctionRunner(
            FunctionId functionId,
            IFunctionStore functionStore,
            Func<TParam, Task<TReturn>> func, TParam param,
            SignOfLifeUpdaterFactory signOfLifeUpdaterFactory)
        {
            _functionId = functionId;

            _func = func;
            _param = param;
            _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;

            _functionStore = functionStore;
        }

        public async Task<TReturn> InvokeMethodAndStoreResult()
        {
            var paramJson = JsonSerializer.Serialize(_param);
            var paramType = _param.GetType().SimpleQualifiedName();
            var signOfLife = DateTime.UtcNow.Ticks;
            var created = await _functionStore.StoreFunction(
                _functionId,
                param1: new Parameter(paramJson, paramType),
                param2: null,
                scrapbookType: null,
                signOfLife
            );

            if (!created)
                return await WaitForFunctionResult(_functionId, _functionStore);

            using var signOfLifeUpdater =
                _signOfLifeUpdaterFactory.CreateAndStart(_functionId, signOfLife);

            var result = await _func(_param);
            var resultJson = JsonSerializer.Serialize(result);
            var resultType = result.GetType().SimpleQualifiedName();
            await _functionStore.StoreFunctionResult(_functionId, resultJson, resultType);

            return result;
        }
        
        private static async Task<TReturn> WaitForFunctionResult(FunctionId functionId, IFunctionStore functionStore)
        {
            while (true)
            {
                var possibleResult = await functionStore.GetFunctionResult(functionId);
                if (possibleResult != null)
                    return JsonSerializer.Deserialize<TReturn>(possibleResult.ResultJson)!;

                await Task.Delay(100);
            }
        }
    }

    internal class RFunctionRunner<TParam1, TParam2, TReturn>
        where TParam1 : notnull where TParam2 : notnull where TReturn : notnull
    {
        private readonly FunctionId _functionId;
        private readonly Func<TParam1, TParam2, Task<TReturn>> _func;
        private readonly TParam1 _param1;
        private readonly TParam2 _param2;

        private readonly IFunctionStore _functionStore;
        private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;

        public RFunctionRunner(
            FunctionId functionId,
            IFunctionStore functionStore,
            Func<TParam1, TParam2, Task<TReturn>> func, TParam1 param1, TParam2 param2,
            SignOfLifeUpdaterFactory signOfLifeUpdaterFactory)
        {
            _functionId = functionId;

            _func = func;
            _param1 = param1;
            _param2 = param2;
            _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;

            _functionStore = functionStore;
        }

        public async Task<TReturn> InvokeMethodAndStoreResult()
        {
            var param1Json = JsonSerializer.Serialize(_param1);
            var param1Type = _param1.GetType().SimpleQualifiedName();
            var param2Json = JsonSerializer.Serialize(_param2);
            var param2Type = _param2.GetType().SimpleQualifiedName();
            var signOfLife = DateTime.UtcNow.Ticks;
            var created = await _functionStore.StoreFunction(
                _functionId,
                param1: new Parameter(param1Json, param1Type),
                param2: new Parameter(param2Json, param2Type),
                scrapbookType: null,
                signOfLife
            );

            if (!created)
                return await WaitForFunctionResult(_functionId, _functionStore);

            using var signOfLifeUpdater =
                _signOfLifeUpdaterFactory.CreateAndStart(_functionId, signOfLife);

            var result = await _func(_param1, _param2);
            var resultJson = JsonSerializer.Serialize(result);
            var resultType = result.GetType().SimpleQualifiedName();
            await _functionStore.StoreFunctionResult(_functionId, resultJson, resultType);

            return result;
        }
        
        private static async Task<TReturn> WaitForFunctionResult(FunctionId functionId, IFunctionStore functionStore)
        {
            while (true)
            {
                var possibleResult = await functionStore.GetFunctionResult(functionId);
                if (possibleResult != null)
                    return JsonSerializer.Deserialize<TReturn>(possibleResult.ResultJson)!;

                await Task.Delay(100);
            }
        }
    }

    internal class RFunctionRunnerWithScrapbook<TParam, TScrapbook, TReturn>
        where TParam : notnull
        where TScrapbook : RScrapbook, new()
        where TReturn : notnull
    {
        private readonly FunctionId _functionId;
        private readonly Func<TParam, TScrapbook, Task<TReturn>> _func;
        private readonly TParam _param;

        private readonly IFunctionStore _functionStore;
        private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;

        public RFunctionRunnerWithScrapbook(
            FunctionId functionId,
            IFunctionStore functionStore,
            Func<TParam, TScrapbook, Task<TReturn>> func, TParam param,
            SignOfLifeUpdaterFactory signOfLifeUpdaterFactory)
        {
            _functionId = functionId;

            _func = func;
            _param = param;
            _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;

            _functionStore = functionStore;
        }

        public async Task<TReturn> InvokeMethodAndStoreResult()
        {
            var paramJson = JsonSerializer.Serialize(_param);
            var paramType = _param.GetType().SimpleQualifiedName();
            var signOfLife = DateTime.UtcNow.Ticks;
            var created = await _functionStore.StoreFunction(
                _functionId,
                param1 : new Parameter(paramJson, paramType),
                param2: null,
                scrapbookType: typeof(TScrapbook).SimpleQualifiedName(),
                signOfLife
            );

            if (!created)
                return await WaitForFunctionResult(_functionId, _functionStore);

            using var signOfLifeUpdater =
                _signOfLifeUpdaterFactory.CreateAndStart(_functionId, signOfLife);

            var scrapbook = new TScrapbook();
            scrapbook.Initialize(_functionId, _functionStore, 0);
            
            var result = await _func(_param, scrapbook);
            
            var resultJson = JsonSerializer.Serialize(result);
            var resultType = result.GetType().SimpleQualifiedName();
            await _functionStore.StoreFunctionResult(_functionId, resultJson, resultType);

            return result;
        }
        
        private static async Task<TReturn> WaitForFunctionResult(FunctionId functionId, IFunctionStore functionStore)
        {
            while (true)
            {
                var possibleResult = await functionStore.GetFunctionResult(functionId);
                if (possibleResult != null)
                    return JsonSerializer.Deserialize<TReturn>(possibleResult.ResultJson)!;

                await Task.Delay(100);
            }
        }
    }
}
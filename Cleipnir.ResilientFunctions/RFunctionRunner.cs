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
        private readonly FunctionTypeId _functionTypeId;
        private readonly Func<TParam, object> _idFunc;
        private readonly Func<TParam, Task<TReturn>> _func;

        private readonly IFunctionStore _functionStore;
        private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;

        public RFunctionRunner(
            FunctionTypeId functionTypeId,
            IFunctionStore functionStore,
            Func<TParam, Task<TReturn>> func, 
            Func<TParam, object> idFunc,
            SignOfLifeUpdaterFactory signOfLifeUpdaterFactory 
        )
        {
            _functionTypeId = functionTypeId;

            _func = func;
            _idFunc = idFunc;
            
            _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
            _functionStore = functionStore;
        }

        public async Task<TReturn> InvokeRFunc(TParam param)
        {
            var functionId = new FunctionId(_functionTypeId, _idFunc(param).ToString()!.ToFunctionInstanceId());
            var (hasResult, result, signOfLifeUpdater) = await InitializeAndFetchAnyExistingResult(functionId, param);
            
            if (hasResult) return result!;
            
            using var _= signOfLifeUpdater!;

            result = await _func(param); //invoking the actual function

            await StoreResult(functionId, result);

            return result;
        }

        private async Task<InitializationResult> InitializeAndFetchAnyExistingResult(FunctionId functionId, TParam param)
        {
            var paramJson = JsonSerializer.Serialize(param);
            var paramType = param.GetType().SimpleQualifiedName();
            var signOfLife = DateTime.UtcNow.Ticks;
            var created = await _functionStore.StoreFunction(
                functionId,
                param1: new Parameter(paramJson, paramType),
                param2: null,
                scrapbookType: null,
                signOfLife
            );

            if (!created)
            {
                var result = await WaitForFunctionResult(functionId);
                return new InitializationResult(true, result, null);
            }
                
            var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, signOfLife);

            return new InitializationResult(false, default, signOfLifeUpdater);
        }

        private async Task StoreResult(FunctionId functionId, TReturn result)
        {
            var resultJson = JsonSerializer.Serialize(result);
            var resultType = result.GetType().SimpleQualifiedName();
            await _functionStore.StoreFunctionResult(functionId, resultJson, resultType);
        }

        private async Task<TReturn> WaitForFunctionResult(FunctionId functionId)
        {
            while (true)
            {
                var possibleResult = await _functionStore.GetFunctionResult(functionId);
                if (possibleResult != null)
                    return JsonSerializer.Deserialize<TReturn>(possibleResult.ResultJson)!;

                await Task.Delay(100);
            }
        }
        
        private record struct InitializationResult(bool HasResult, TReturn? Result, IDisposable? SignOfLifeUpdater);
    }

    internal class RFunctionRunner<TParam1, TParam2, TReturn>
        where TParam1 : notnull where TParam2 : notnull where TReturn : notnull
    {
        private readonly FunctionTypeId _functionTypeId;
        private readonly Func<TParam1, object> _idFunc;
        private readonly Func<TParam1, TParam2, Task<TReturn>> _func;

        private readonly IFunctionStore _functionStore;
        private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;

        public RFunctionRunner(
            FunctionTypeId functionTypeId,
            IFunctionStore functionStore,
            Func<TParam1, TParam2, Task<TReturn>> func, 
            Func<TParam1, object> idFunc,
            SignOfLifeUpdaterFactory signOfLifeUpdaterFactory 
        )
        {
            _functionTypeId = functionTypeId;

            _func = func;
            _idFunc = idFunc;
            
            _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
            _functionStore = functionStore;
        }

        public async Task<TReturn> InvokeRFunc(TParam1 param1, TParam2 param2)
        {
            var functionId = new FunctionId(_functionTypeId, _idFunc(param1).ToString()!.ToFunctionInstanceId());
            var (hasResult, result, signOfLifeUpdater) = 
                await InitializeAndFetchAnyExistingResult(functionId, param1, param2);
            
            if (hasResult) return result!;
            
            using var _= signOfLifeUpdater!;

            result = await _func(param1, param2); //invoking the actual function

            await StoreResult(functionId, result);

            return result;
        }

        private async Task<InitializationResult> InitializeAndFetchAnyExistingResult(
            FunctionId functionId, TParam1 param1, TParam2 param2
        )
        {
            var param1Json = JsonSerializer.Serialize(param1);
            var param1Type = param1.GetType().SimpleQualifiedName();
            var param2Json = JsonSerializer.Serialize(param2);
            var param2Type = param2.GetType().SimpleQualifiedName();
            var signOfLife = DateTime.UtcNow.Ticks;
            var created = await _functionStore.StoreFunction(
                functionId,
                param1: new Parameter(param1Json, param1Type),
                param2: new Parameter(param2Json, param2Type),
                scrapbookType: null,
                signOfLife
            );

            if (!created)
            {
                var result = await WaitForFunctionResult(functionId);
                return new InitializationResult(true, result, null);
            }
                
            var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, signOfLife);

            return new InitializationResult(false, default, signOfLifeUpdater);
        }

        private async Task StoreResult(FunctionId functionId, TReturn result)
        {
            var resultJson = JsonSerializer.Serialize(result);
            var resultType = result.GetType().SimpleQualifiedName();
            await _functionStore.StoreFunctionResult(functionId, resultJson, resultType);
        }

        private async Task<TReturn> WaitForFunctionResult(FunctionId functionId)
        {
            while (true)
            {
                var possibleResult = await _functionStore.GetFunctionResult(functionId);
                if (possibleResult != null)
                    return JsonSerializer.Deserialize<TReturn>(possibleResult.ResultJson)!;

                await Task.Delay(100);
            }
        }
        
        private record struct InitializationResult(bool HasResult, TReturn? Result, IDisposable? SignOfLifeUpdater);
    }

    internal class RFunctionRunnerWithScrapbook<TParam, TScrapbook, TReturn>
        where TParam : notnull
        where TScrapbook : RScrapbook, new()
        where TReturn : notnull
    {
        private readonly FunctionTypeId _functionTypeId;
        private readonly Func<TParam, object> _idFunc;
        private readonly Func<TParam, TScrapbook, Task<TReturn>> _func;

        private readonly IFunctionStore _functionStore;
        private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;

        public RFunctionRunnerWithScrapbook(
            FunctionTypeId functionTypeId,
            IFunctionStore functionStore,
            Func<TParam, TScrapbook, Task<TReturn>> func, 
            Func<TParam, object> idFunc,
            SignOfLifeUpdaterFactory signOfLifeUpdaterFactory 
        )
        {
            _functionTypeId = functionTypeId;

            _func = func;
            _idFunc = idFunc;
            
            _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
            _functionStore = functionStore;
        }

        public async Task<TReturn> InvokeRFunc(TParam param)
        {
            var functionId = new FunctionId(_functionTypeId, _idFunc(param).ToString()!.ToFunctionInstanceId());
            var (hasResult, result, scrapbook, signOfLifeUpdater) = await InitializeAndFetchAnyExistingResult(functionId, param);
            
            if (hasResult) return result!;
            
            using var _= signOfLifeUpdater!;
            
            result = await _func(param, scrapbook!); //invoking the actual function

            await StoreResult(functionId, result);

            return result;
        }

        private async Task<InitializationResult> InitializeAndFetchAnyExistingResult(FunctionId functionId, TParam param)
        {
            var paramJson = JsonSerializer.Serialize(param);
            var paramType = param.GetType().SimpleQualifiedName();
            var signOfLife = DateTime.UtcNow.Ticks;
            var created = await _functionStore.StoreFunction(
                functionId,
                param1: new Parameter(paramJson, paramType),
                param2: null,
                scrapbookType: typeof(TScrapbook).SimpleQualifiedName(),
                signOfLife
            );

            if (!created)
            {
                var result = await WaitForFunctionResult(functionId);
                return new InitializationResult(true, result, null, null);
            }
                
            var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, signOfLife);

            var scrapbook = new TScrapbook();
            scrapbook.Initialize(functionId, _functionStore, 0);
            return new InitializationResult(false, default, scrapbook, signOfLifeUpdater);
        }

        private async Task StoreResult(FunctionId functionId, TReturn result)
        {
            var resultJson = JsonSerializer.Serialize(result);
            var resultType = result.GetType().SimpleQualifiedName();
            await _functionStore.StoreFunctionResult(functionId, resultJson, resultType);
        }

        private async Task<TReturn> WaitForFunctionResult(FunctionId functionId)
        {
            while (true)
            {
                var possibleResult = await _functionStore.GetFunctionResult(functionId);
                if (possibleResult != null)
                    return JsonSerializer.Deserialize<TReturn>(possibleResult.ResultJson)!;

                await Task.Delay(100);
            }
        }

        private record struct InitializationResult(
            bool HasResult, TReturn? Result,
            TScrapbook? Scrapbook,
            IDisposable? SignOfLifeUpdater
        );
    }
}
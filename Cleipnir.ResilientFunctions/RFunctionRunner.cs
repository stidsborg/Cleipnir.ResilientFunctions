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
                paramJson,
                paramType,
                signOfLife
            );

            if (!created)
                return await WaitForFunctionResult();

            using var signOfLifeUpdater = 
                _signOfLifeUpdaterFactory.CreateAndStart(_functionId, signOfLife);
            
            var result = await _func(_param);
            var resultJson = JsonSerializer.Serialize(result);
            var resultType = result.GetType().SimpleQualifiedName();
            await _functionStore.StoreFunctionResult(_functionId, resultJson, resultType);

            return result;
        }

        private async Task<TReturn> WaitForFunctionResult()
        {
            while (true)
            {
                var possibleResult = await _functionStore.GetFunctionResult(_functionId);
                if (possibleResult != null)
                    return JsonSerializer.Deserialize<TReturn>(possibleResult.ResultJson)!;

                await Task.Delay(100);
            }
        }
    }
}
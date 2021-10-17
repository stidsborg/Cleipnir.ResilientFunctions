using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests
{
    public abstract class UnhandledWatchdogTests
    {
        private readonly FunctionTypeId _functionTypeId = "functionId".ToFunctionTypeId();
        private readonly FunctionInstanceId _instanceId = "instanceId".ToFunctionInstanceId();
        private FunctionId FunctionId => new FunctionId(_functionTypeId, _instanceId);

        public abstract Task UnhandledFunctionInvocationIsCompletedByWatchDog();
        
        public async Task UnhandledFunctionInvocationIsCompletedByWatchDog(IFunctionStore store)
        {
            using var watchDog = new UnhandledWatchdog<string, string>(
                _functionTypeId,
                s => Task.FromResult(s.ToUpper()),
                store,
                new SignOfLifeUpdaterFactory(store, TimeSpan.Zero),
                TimeSpan.FromMilliseconds(1)
            );

            await store.StoreFunction(
                FunctionId,
                "hello".ToJson(),
                typeof(string).SimpleQualifiedName(),
                DateTime.UtcNow.AddMinutes(-1).Ticks
            );

            _ = watchDog.Start();

            FunctionResult? functionResult = null;
            await BusyWait.Until(
                async () => (functionResult = await store.GetFunctionResult(FunctionId)) != null
            );
            
            functionResult!.Deserialize().ShouldBe("HELLO");
        }
    }
}
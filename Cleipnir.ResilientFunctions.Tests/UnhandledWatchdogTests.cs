using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Shouldly;
using static Cleipnir.ResilientFunctions.Tests.Utils.TestUtils;

namespace Cleipnir.ResilientFunctions.Tests
{
    public abstract class UnhandledWatchdogTests
    {
        private readonly FunctionTypeId _functionTypeId = "functionId".ToFunctionTypeId();
        private readonly FunctionInstanceId _instanceId = "instanceId".ToFunctionInstanceId();
        private FunctionId FunctionId => new FunctionId(_functionTypeId, _instanceId);

        public abstract Task UnhandledFunctionInvocationIsCompletedByWatchDog();

        protected async Task UnhandledFunctionInvocationIsCompletedByWatchDog(IFunctionStore store)
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
            
            using var watchDog = new UnhandledRFunctionWatchdog<string, string>(
                _functionTypeId,
                RFunc.ToUpper,
                store,
                CreateNeverExecutionSignOfLifeUpdaterFactory(),
                TimeSpan.FromMilliseconds(1),
                unhandledExceptionCatcher.Catch
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
            unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
        }
    }
}
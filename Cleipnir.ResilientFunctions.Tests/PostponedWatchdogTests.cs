using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Watchdogs;
using Cleipnir.ResilientFunctions.Watchdogs.Invocation;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests
{
    public abstract class PostponedWatchdogTests
    {
        private readonly FunctionTypeId _functionTypeId = "functionId".ToFunctionTypeId();
        private readonly FunctionInstanceId _instanceId = "instanceId".ToFunctionInstanceId();
        private FunctionId FunctionId => new FunctionId(_functionTypeId, _instanceId);

        public abstract Task PostponedFunctionInvocationIsCompletedByWatchDog();
        protected async Task PostponedFunctionInvocationIsCompletedByWatchDog(IFunctionStore store)
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
            
            using var watchDog = new PostponedWatchdog<string>(
                _functionTypeId,
                (param, _) => RFunc.ToUpper(param.ToString()!),
                store,
                new RFuncInvoker(
                    store, 
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    unhandledExceptionCatcher.Catch
                ),
                checkFrequency: TimeSpan.FromMilliseconds(1),
                unhandledExceptionCatcher.Catch
            );

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            await store.SetFunctionState(
                FunctionId,
                Status.Postponed,
                scrapbookJson: null,
                result: null,
                failed: null,
                postponedUntil: DateTime.UtcNow.AddMinutes(-1).Ticks,
                expectedEpoch: 0
            ).ShouldBeTrueAsync();
            
            _ = watchDog.Start();
            
            await BusyWait.Until(
                async () => (await store.GetFunctionsWithStatus(_functionTypeId, Status.Succeeded)).Any()
            );

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction!.Result!.Deserialize().ShouldBe("HELLO");
            
            unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
        }
    }
}
using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Watchdogs;
using Cleipnir.ResilientFunctions.Watchdogs.Invocation;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates
{
    public abstract class CrashedWatchdogTests
    {
        private readonly FunctionTypeId _functionTypeId = "functionId".ToFunctionTypeId();
        private readonly FunctionInstanceId _instanceId = "instanceId".ToFunctionInstanceId();
        private FunctionId FunctionId => new FunctionId(_functionTypeId, _instanceId);

        public abstract Task CrashedFunctionInvocationIsCompletedByWatchDog();
        protected async Task CrashedFunctionInvocationIsCompletedByWatchDog(IFunctionStore store)
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
            
            using var watchDog = new CrashedWatchdog<string>(
                _functionTypeId,
                (param, _) => Funcs.ToUpper(param.ToString()!),
                store,
                new RFuncInvoker(
                    store, 
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch)
                ),
                checkFrequency: TimeSpan.FromMilliseconds(1),
                new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch)
            );

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            _ = watchDog.Start();

            await BusyWait.Until(
                async () => await store.GetFunctionsWithStatus(_functionTypeId, Status.Succeeded).Any()
            );

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction!.Result!.Deserialize().ShouldBe("HELLO");
            
            unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
        }
    }
}
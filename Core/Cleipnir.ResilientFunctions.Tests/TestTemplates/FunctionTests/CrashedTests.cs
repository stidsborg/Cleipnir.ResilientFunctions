using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Newtonsoft.Json.Converters;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class CrashedTests
{
    public abstract Task NonCompletedFuncIsCompletedByWatchDog();
    protected async Task NonCompletedFuncIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        const string param = "test";
        {
            var crashableStore = store.ToCrashableFunctionStore();
            var registry = new FunctionsRegistry(crashableStore);
            var func = registry
                .RegisterFunc(
                    flowType,
                    (string _) => NeverCompletingTask.OfType<string>()
                ).Schedule;

            await func(flowInstance.Value, param);
            crashableStore.Crash();
        }
        {
            using var functionsRegistry = new FunctionsRegistry(store);
            var registration = functionsRegistry
                .RegisterFunc(
                    flowType,
                    (string s) => s.ToUpper().ToTask()
                );
            var rFunc = registration.Invoke;
            
            await BusyWait.Until(
                async () => await store
                    .GetFunction(registration.MapToStoredId(functionId.Instance))
                    .Map(f => f?.Status == Status.Succeeded)
            );

            var status = await store.GetFunction(registration.MapToStoredId(functionId.Instance)).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            await rFunc(flowInstance.Value, param).ShouldBeAsync("TEST");
        }
    }
    
    public abstract Task NonCompletedActionIsCompletedByWatchDog();
    protected async Task NonCompletedActionIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        const string param = "test";
        {
            var crashableStore = store.ToCrashableFunctionStore();
            using var nonCompletingFunctionsRegistry = new FunctionsRegistry(crashableStore);

            await nonCompletingFunctionsRegistry.RegisterAction(
                flowType,
                (string _) => NeverCompletingTask.OfVoidType
            ).Schedule(flowInstance, param);
            crashableStore.Crash();
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(leaseLength: TimeSpan.FromMilliseconds(250))
            );

            var registration = functionsRegistry
                .RegisterAction(
                    flowType,
                    (string _) => Task.CompletedTask
                );
            var rAction = registration.Invoke;
            
            await BusyWait.Until(
                async () => await store
                    .GetFunction(registration.MapToStoredId(functionId.Instance))
                    .Map(f => f?.Status ?? Status.Failed) == Status.Succeeded
            );

            var status = await store.GetFunction(registration.MapToStoredId(functionId.Instance)).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            await rAction(flowInstance.Value, param);
        }
    }
}
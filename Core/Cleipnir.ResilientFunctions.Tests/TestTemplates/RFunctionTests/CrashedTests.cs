using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class CrashedTests
{
    public abstract Task NonCompletedFuncIsCompletedByWatchDog();
    protected async Task NonCompletedFuncIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new FunctionsRegistry
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        enableWatchdogs: false,
                        leaseLength: TimeSpan.Zero
                    )
                )
                .RegisterFunc(
                    flowType,
                    (string _) => NeverCompletingTask.OfType<string>()
                ).Invoke;

            _ = nonCompletingRFunctions(flowInstance.Value, param);
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromSeconds(1)
                )
            );

            var registration = functionsRegistry
                .RegisterFunc(
                    flowType,
                    (string s) => s.ToUpper().ToTask()
                );
            var rFunc = registration.Invoke;
            
            await BusyWait.Until(
                async () => await store
                    .GetFunction(registration.MapToStoredId(functionId))
                    .Map(f => f?.Status == Status.Succeeded)
            );

            var status = await store.GetFunction(registration.MapToStoredId(functionId)).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            await rFunc(flowInstance.Value, param).ShouldBeAsync("TEST");
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }

    public abstract Task NonCompletedFuncWithStateIsCompletedByWatchDog();
    protected async Task NonCompletedFuncWithStateIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            using var functionsRegistry = new FunctionsRegistry
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        leaseLength: TimeSpan.Zero, 
                        enableWatchdogs: false
                    )
                );
            var nonCompletingFunctionsRegistry = functionsRegistry    
                .RegisterFunc(
                    flowType,
                    (string _) => NeverCompletingTask.OfType<Result<string>>()
                ).Invoke;

            _ = nonCompletingFunctionsRegistry(flowInstance.Value, param);
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(250)
                )
            );

            var registration = functionsRegistry
                .RegisterFunc(
                    flowType,
                    async (string s, Workflow workflow) =>
                    {
                        var state = await workflow.States.CreateOrGet<State>("State");
                        state.Value = 1;
                        await state.Save();
                        return s.ToUpper();
                    }
                );
            var rFunc = registration.Invoke;
            
            await BusyWait.Until(async () => 
                await store
                    .GetFunction(registration.MapToStoredId(functionId))
                    .Map(f => f?.Status == Status.Succeeded),
                maxWait: TimeSpan.FromSeconds(5)
            );

            var storedFunction = await store.GetFunction(registration.MapToStoredId(functionId));
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            var effects = await store.EffectsStore.GetEffectResults(registration.MapToStoredId(functionId));
            var stateResult = effects.Single(e => e.EffectId == "State").Result!;
            stateResult.ShouldNotBeNull();
            stateResult.ToStringFromUtf8Bytes().DeserializeFromJsonTo<State>().Value.ShouldBe(1);
            await rFunc(flowInstance.Value, param).ShouldBeAsync("TEST");
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
    
    public abstract Task NonCompletedActionIsCompletedByWatchDog();
    protected async Task NonCompletedActionIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingFunctionsRegistry = new FunctionsRegistry
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        leaseLength: TimeSpan.Zero, 
                        enableWatchdogs: false
                    )
                )
                .RegisterAction(
                    flowType,
                    (string _) => NeverCompletingTask.OfVoidType
                )
                .Invoke;

            _ = nonCompletingFunctionsRegistry(flowInstance.Value, param);
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(250)
                )
            );

            var registration = functionsRegistry
                .RegisterAction(
                    flowType,
                    (string _) => Task.CompletedTask
                );
            var rAction = registration.Invoke;
            
            await BusyWait.Until(
                async () => await store
                    .GetFunction(registration.MapToStoredId(functionId))
                    .Map(f => f?.Status ?? Status.Failed) == Status.Succeeded
            );

            var status = await store.GetFunction(registration.MapToStoredId(functionId)).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            await rAction(flowInstance.Value, param);
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }

    public abstract Task NonCompletedActionWithStateIsCompletedByWatchDog();
    protected async Task NonCompletedActionWithStateIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingFunctionsRegistry = new FunctionsRegistry
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        leaseLength: TimeSpan.Zero, 
                        enableWatchdogs: false
                    )
                )
                .RegisterAction(
                    flowType,
                    (string _) => NeverCompletingTask.OfVoidType
                ).Invoke;

            _ = nonCompletingFunctionsRegistry(flowInstance.Value, param);
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(250)
                )
            );

            var registration = functionsRegistry
                .RegisterAction(
                    flowType,
                    async (string _, Workflow workflow) =>
                    {
                        var state = await workflow.States.CreateOrGet<State>("State");
                        state.Value = 1;
                        await state.Save();
                    }
                );
            var rAction = registration.Invoke;

            await BusyWait.Until(async () =>
                await store
                    .GetFunction(registration.MapToStoredId(functionId))
                    .Map(f => f?.Status == Status.Succeeded),
                maxWait: TimeSpan.FromSeconds(5)
            );

            var storedFunction = await store.GetFunction(registration.MapToStoredId(functionId));
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            var effects = await store.EffectsStore.GetEffectResults(registration.MapToStoredId(functionId));
            var state = effects.Single(e => e.EffectId == "State").Result;
            state!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<State>().Value.ShouldBe(1);
            await rAction(flowInstance.Value, param);
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
    
    private class State : FlowState
    {
        public int Value { get; set; }
    }
}
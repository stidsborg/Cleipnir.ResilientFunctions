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
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new FunctionsRegistry
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        leaseLength: TimeSpan.Zero, 
                        watchdogCheckFrequency: TimeSpan.Zero
                    )
                )
                .RegisterFunc(
                    functionTypeId,
                    (string _) => NeverCompletingTask.OfType<string>()
                ).Invoke;

            _ = nonCompletingRFunctions(functionInstanceId.Value, param);
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(1_000)
                )
            );

            var rFunc = functionsRegistry
                .RegisterFunc(
                    functionTypeId,
                    (string s) => s.ToUpper().ToTask()
                ).Invoke;
            
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) == Status.Succeeded,
                maxWait: TimeSpan.FromSeconds(10)
            );

            var status = await store.GetFunction(functionId).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            await rFunc(functionInstanceId.Value, param).ShouldBeAsync("TEST");
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }

    public abstract Task NonCompletedFuncWithStateIsCompletedByWatchDog();
    protected async Task NonCompletedFuncWithStateIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            using var functionsRegistry = new FunctionsRegistry
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        leaseLength: TimeSpan.Zero, 
                        watchdogCheckFrequency: TimeSpan.Zero
                    )
                );
            var nonCompletingFunctionsRegistry = functionsRegistry    
                .RegisterFunc(
                    functionTypeId,
                    (string _) => NeverCompletingTask.OfType<Result<string>>()
                ).Invoke;

            _ = nonCompletingFunctionsRegistry(functionInstanceId.Value, param);
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(250)
                )
            );

            var rFunc = functionsRegistry
                .RegisterFunc(
                    functionTypeId,
                    async (string s, Workflow workflow) =>
                    {
                        var state = workflow.States.CreateOrGet<State>("State");
                        state.Value = 1;
                        await state.Save();
                        return s.ToUpper();
                    }
                ).Invoke;
            
            await BusyWait.Until(async () => 
                await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status == Status.Succeeded),
                maxWait: TimeSpan.FromSeconds(5)
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            var effects = await store.StatesStore.GetStates(functionId);
            var stateResult = effects.Single(e => e.StateId == "State").StateJson;
            stateResult.ShouldNotBeNull();
            stateResult.DeserializeFromJsonTo<State>().Value.ShouldBe(1);
            await rFunc(functionInstanceId.Value, param).ShouldBeAsync("TEST");
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
    
    public abstract Task NonCompletedActionIsCompletedByWatchDog();
    protected async Task NonCompletedActionIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingFunctionsRegistry = new FunctionsRegistry
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        leaseLength: TimeSpan.Zero, 
                        watchdogCheckFrequency: TimeSpan.Zero
                    )
                )
                .RegisterAction(
                    functionTypeId,
                    (string _) => NeverCompletingTask.OfVoidType
                )
                .Invoke;

            _ = nonCompletingFunctionsRegistry(functionInstanceId.Value, param);
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(250)
                )
            );

            var rAction = functionsRegistry
                .RegisterAction(
                    functionTypeId,
                    (string _) => Task.CompletedTask
                )
                .Invoke;
            
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) == Status.Succeeded
            );

            var status = await store.GetFunction(functionId).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            await rAction(functionInstanceId.Value, param);
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }

    public abstract Task NonCompletedActionWithStateIsCompletedByWatchDog();
    protected async Task NonCompletedActionWithStateIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingFunctionsRegistry = new FunctionsRegistry
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        leaseLength: TimeSpan.Zero, 
                        watchdogCheckFrequency: TimeSpan.Zero
                    )
                )
                .RegisterAction(
                    functionTypeId,
                    (string _) => NeverCompletingTask.OfVoidType
                ).Invoke;

            _ = nonCompletingFunctionsRegistry(functionInstanceId.Value, param);
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(250)
                )
            );

            var rAction = functionsRegistry
                .RegisterAction(
                    functionTypeId,
                    async (string _, Workflow workflow) =>
                    {
                        var state = workflow.States.CreateOrGet<State>("State");
                        state.Value = 1;
                        await state.Save();
                    }
                ).Invoke;

            await BusyWait.Until(async () =>
                await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status == Status.Succeeded),
                maxWait: TimeSpan.FromSeconds(5)
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            var effects = await store.StatesStore.GetStates(functionId);
            var state = effects.Single(e => e.StateId == "State").StateJson;
            state.DeserializeFromJsonTo<State>().Value.ShouldBe(1);
            await rAction(functionInstanceId.Value, param);
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
    
    private class State : WorkflowState
    {
        public int Value { get; set; }
    }
}
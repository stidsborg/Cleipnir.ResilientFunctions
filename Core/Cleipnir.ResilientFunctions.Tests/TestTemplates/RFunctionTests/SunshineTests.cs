using System;
using System.Linq;
using System.Threading.Tasks;
using Castle.Components.DictionaryAdapter.Xml;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class SunshineTests
{
    public abstract Task SunshineScenarioFunc();
    public async Task SunshineScenarioFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(SunshineScenarioFunc).ToFlowType();
        async Task<string> ToUpper(string s)
        {
            await Task.Delay(10);
            return s.ToUpper();
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var rFunc = functionsRegistry
            .RegisterFunc(
                flowType,
                (string s) => ToUpper(s)
            ).Invoke;

        var result = await rFunc("hello", "hello");
        result.ShouldBe("HELLO");
            
        var storedFunction = await store.GetFunction(
            new FlowId(
                flowType, 
                "hello".ToFlowInstance()
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldNotBeNull();
        var storedResult = storedFunction.Result.DeserializeFromJsonTo<string>();
        storedResult.ShouldBe("HELLO");
            
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task SunshineScenarioParamless();
    public async Task SunshineScenarioParamless(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = TestFlowId.Create().Type;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var flag = new SyncedFlag();
        var registration = functionsRegistry
            .RegisterParamless(
                flowType,
                inner: () =>
                {
                    flag.Raise();
                    return Task.CompletedTask;
                })
            .Invoke;

        await registration("SomeInstanceId");
        flag.Position.ShouldBe(FlagPosition.Raised);
            
        var storedFunction = await store.GetFunction(new FlowId(flowType, flowInstance: "SomeInstanceId"));
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldBeNull();
        storedFunction.Parameter.ShouldBeNull();
            
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task SunshineScenarioParamlessWithResultReturnType();
    public async Task SunshineScenarioParamlessWithResultReturnType(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = TestFlowId.Create().Type;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var flag = new SyncedFlag();
        var registration = functionsRegistry
            .RegisterParamless(
                flowType,
                inner: () =>
                {
                    flag.Raise();
                    return Result.Succeed.ToTask();
                })
            .Invoke;

        await registration("SomeInstanceId");
        flag.Position.ShouldBe(FlagPosition.Raised);
            
        var storedFunction = await store.GetFunction(new FlowId(flowType, flowInstance: "SomeInstanceId"));
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldBeNull();
        storedFunction.Parameter.ShouldBeNull();
            
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
        
    public abstract Task SunshineScenarioFuncWithState();
    public async Task SunshineScenarioFuncWithState(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(SunshineScenarioFuncWithState).ToFlowType();
        async Task<string> ToUpper(string s, State state)
        {
            await state.Save();
            return s.ToUpper();
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var rFunc = functionsRegistry
            .RegisterFunc(
                flowType,
                async Task<string> (string s, Workflow workflow) => await ToUpper(s, await workflow.States.CreateOrGet<State>("State"))
            )
            .Invoke;

        var result = await rFunc("hello", "hello");
        result.ShouldBe("HELLO");
            
        var storedFunction = await store.GetFunction(
            new FlowId(
                flowType, 
                "hello".ToFlowInstance()
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldNotBeNull();
        var storedResult = storedFunction.Result.DeserializeFromJsonTo<string>();
        storedResult.ShouldBe("HELLO");
            
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
        
    public abstract Task SunshineScenarioAction();
    public async Task SunshineScenarioAction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(SunshineScenarioAction).ToFlowType();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var rAction = functionsRegistry
            .RegisterAction(
                flowType,
                (string _) => Task.Delay(10)
            )
            .Invoke;

        await rAction("hello", "hello");
        
        var storedFunction = await store.GetFunction(
            new FlowId(
                flowType, 
                "hello".ToFlowInstance()
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
        
    public abstract Task SunshineScenarioActionWithState();
    public async Task SunshineScenarioActionWithState(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(SunshineScenarioActionWithState).ToFlowType();

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var rFunc = functionsRegistry
            .RegisterAction(
                flowType,
                async (string s, Workflow workflow) => await (await workflow.States.CreateOrGet<State>("State")).Save()
            ).Invoke;

        await rFunc("hello", "hello");

        var storedFunction = await store.GetFunction(
            new FlowId(
                flowType, 
                "hello".ToFlowInstance()
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task SunshineScenarioNullReturningFunc();
    protected async Task SunshineScenarioNullReturningFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        FlowType flowType = "SomeFunctionType";
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            (string s) => default(string).ToTask()
        ).Invoke;

        var result = await rFunc("hello world", "hello world");
        result.ShouldBeNull();
    }

    public abstract Task SunshineScenarioNullReturningFuncWithState();
    protected async Task SunshineScenarioNullReturningFuncWithState(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            async (string _, Workflow workflow) =>
            {
                var state = await workflow.States.CreateOrGet<ListState<string>>("State");
                state.List.Add("hello world");
                state.Save().Wait();
                return default(string);
            }
        ).Invoke;

        var result = await rFunc(flowInstance.Value, "hello world");
        result.ShouldBeNull();

        var storedFunction = await store
            .GetFunction(functionId)
            .ShouldNotBeNullAsync();

        var states = await store.StatesStore.GetStates(functionId);
        var state = states.Single(e => e.StateId == "State").StateJson!.DeserializeFromJsonTo<ListState<string>>();
        state.List.Single().ShouldBe("hello world");
    }
    
    public abstract Task SecondInvocationOnNullReturningFuncReturnsNullSuccessfully();
    protected async Task SecondInvocationOnNullReturningFuncReturnsNullSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        FlowType flowType = "SomeFunctionType";
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            async (string _, Workflow workflow) =>
            {
                var state = await workflow.States.CreateOrGet<ListState<string>>("State");
                state.List.Add("hello world");
                state.Save().Wait();
                return default(string);
            }
        ).Invoke;

        var result = await rFunc("hello world", "hello world");
        result.ShouldBeNull();

        result = await rFunc("hello world", "hello world");
        result.ShouldBeNull();
    }
    
    public abstract Task FunctionIsRemovedAfterRetentionPeriod();
    protected async Task FunctionIsRemovedAfterRetentionPeriod(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var functionId = TestFlowId.Create();
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    enableWatchdogs: false
                )
            );
        
            var rFunc = functionsRegistry.RegisterAction(
                functionId.Type,
                inner: (string _) => Task.CompletedTask
            ).Invoke;

            await rFunc("hello world", "hello world");
        }

        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    enableWatchdogs: true,
                    retentionPeriod: TimeSpan.Zero
                )
            );

            functionsRegistry.RegisterAction(
                functionId.Type,
                inner: (string _) => Task.CompletedTask
            );
            
            await BusyWait.Until(async () => await store.GetFunction(functionId) is null);
        }
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    private class State : Domain.FlowState {}
}
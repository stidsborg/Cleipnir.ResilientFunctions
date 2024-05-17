using System.Linq;
using System.Threading.Tasks;
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
        var functionTypeId = nameof(SunshineScenarioFunc).ToFunctionTypeId();
        async Task<string> ToUpper(string s)
        {
            await Task.Delay(10);
            return s.ToUpper();
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var rFunc = functionsRegistry
            .RegisterFunc(
                functionTypeId,
                (string s) => ToUpper(s)
            ).Invoke;

        var result = await rFunc("hello", "hello");
        result.ShouldBe("HELLO");
            
        var storedFunction = await store.GetFunction(
            new FunctionId(
                functionTypeId, 
                "hello".ToFunctionInstanceId()
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldNotBeNull();
        var storedResult = storedFunction.Result.DeserializeFromJsonTo<string>();
        storedResult.ShouldBe("HELLO");
            
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task SunshineScenarioParamless();
    public async Task SunshineScenarioParamless(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = TestFunctionId.Create().TypeId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var flag = new SyncedFlag();
        var registration = functionsRegistry
            .RegisterParamless(
                functionTypeId,
                inner: () =>
                {
                    flag.Raise();
                    return Task.CompletedTask;
                })
            .Invoke;

        await registration("SomeInstanceId");
        flag.Position.ShouldBe(FlagPosition.Raised);
            
        var storedFunction = await store.GetFunction(new FunctionId(functionTypeId, functionInstanceId: "SomeInstanceId"));
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldBeNull();
        storedFunction.Parameter.ShouldBeNull();
            
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
        
    public abstract Task SunshineScenarioFuncWithState();
    public async Task SunshineScenarioFuncWithState(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(SunshineScenarioFuncWithState).ToFunctionTypeId();
        async Task<string> ToUpper(string s, State state)
        {
            await state.Save();
            return s.ToUpper();
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var rFunc = functionsRegistry
            .RegisterFunc(
                functionTypeId,
                (string s, Workflow workflow) => ToUpper(s, workflow.States.CreateOrGet<State>("State"))
            )
            .Invoke;

        var result = await rFunc("hello", "hello");
        result.ShouldBe("HELLO");
            
        var storedFunction = await store.GetFunction(
            new FunctionId(
                functionTypeId, 
                "hello".ToFunctionInstanceId()
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldNotBeNull();
        var storedResult = storedFunction.Result.DeserializeFromJsonTo<string>();
        storedResult.ShouldBe("HELLO");
            
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
        
    public abstract Task SunshineScenarioAction();
    public async Task SunshineScenarioAction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(SunshineScenarioAction).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var rAction = functionsRegistry
            .RegisterAction(
                functionTypeId,
                (string _) => Task.Delay(10)
            )
            .Invoke;

        await rAction("hello", "hello");
        
        var storedFunction = await store.GetFunction(
            new FunctionId(
                functionTypeId, 
                "hello".ToFunctionInstanceId()
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
        
    public abstract Task SunshineScenarioActionWithState();
    public async Task SunshineScenarioActionWithState(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(SunshineScenarioActionWithState).ToFunctionTypeId();

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var rFunc = functionsRegistry
            .RegisterAction(
                functionTypeId,
                (string s, Workflow workflow) => workflow.States.CreateOrGet<State>("State").Save()
            ).Invoke;

        await rFunc("hello", "hello");

        var storedFunction = await store.GetFunction(
            new FunctionId(
                functionTypeId, 
                "hello".ToFunctionInstanceId()
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task SunshineScenarioNullReturningFunc();
    protected async Task SunshineScenarioNullReturningFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        FunctionTypeId functionTypeId = "SomeFunctionType";
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
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
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            (string _, Workflow workflow) =>
            {
                var state = workflow.States.CreateOrGet<ListState<string>>("State");
                state.List.Add("hello world");
                state.Save().Wait();
                return default(string).ToTask();
            }
        ).Invoke;

        var result = await rFunc(functionInstanceId.Value, "hello world");
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
        FunctionTypeId functionTypeId = "SomeFunctionType";
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            (string _, Workflow workflow) =>
            {
                var state = workflow.States.CreateOrGet<ListState<string>>("State");
                state.List.Add("hello world");
                state.Save().Wait();
                return default(string).ToTask();
            }
        ).Invoke;

        var result = await rFunc("hello world", "hello world");
        result.ShouldBeNull();

        result = await rFunc("hello world", "hello world");
        result.ShouldBeNull();
    }
    
    public abstract Task InvocationModeShouldBeDirectInSunshineScenario();
    protected async Task InvocationModeShouldBeDirectInSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        FunctionTypeId functionTypeId = "SomeFunctionType";
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var syncedInvocationMode = new Synced<InvocationMode>();
        var rFunc = functionsRegistry.RegisterAction(
            functionTypeId,
            (string _) => syncedInvocationMode.Value = InvocationMode.Direct
        ).Invoke;

        await rFunc("hello world", "hello world");
        
        syncedInvocationMode.Value.ShouldBe(InvocationMode.Direct);
    }

    private class State : Domain.WorkflowState {}
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class ReInvocationTests
{
    public abstract Task ActionReInvocationSunshineScenario();
    protected async Task ActionReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );
        var syncedParameter = new Synced<string>();

        var rFunc = functionsRegistry
            .RegisterAction(
                functionType, (string s) =>
                {
                    if (flag.Position == FlagPosition.Lowered)
                    {
                        flag.Raise();
                        throw new Exception("oh no");
                    }

                    syncedParameter.Value = s;
                }
            );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke("something", "something"));

        await rFunc.ControlPanel("something").Result!.ReInvoke();
        
        syncedParameter.Value.ShouldBe("something");

        var function = await store.GetFunction(new FunctionId(functionType, "something"));
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ActionWithStateReInvocationSunshineScenario();
    protected async Task ActionWithStateReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rAction = functionsRegistry.RegisterAction<string, ListState<string>>(
            functionTypeId,
            async (param, state) =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    state.List.Add("hello");
                    await state.Save();
                    flag.Raise();
                    throw new Exception("oh no");
                }
                state.List.Add("world");
            }
        );

        await Should.ThrowAsync<Exception>(() =>
            rAction.Invoke(functionInstanceId.Value, "something")
        );

        var syncedListFromState = new Synced<List<string>>();
        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
            
        syncedListFromState.Value = new List<string>(controlPanel.State.List);
        controlPanel.State.List.Clear();
        await controlPanel.SaveChanges();
        
        controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.ReInvoke();
        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var state = function.State.StateJson.DeserializeFromJsonTo<ListState<string>>();
        state.List.Single().ShouldBe("world");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario();
    protected async Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var syncedParam = new Synced<object>();
        var rAction = functionsRegistry.RegisterAction<object>(
            functionTypeId,
            param =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    flag.Raise();
                    throw new Exception("oh no");
                }
                
                syncedParam.Value = param;
            }
        );

        await Should.ThrowAsync<Exception>(() =>
            rAction.Invoke(functionInstanceId.Value, "something")
        );
        
        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Param.ShouldBe("something");
        controlPanel.Param = 10;
        await controlPanel.SaveChanges();
       
        controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.ReInvoke();
        
        syncedParam.Value.ShouldBe(10);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task UpdatedParameterAndStateIsPassedInOnReInvocationSunshineScenario();
    protected async Task UpdatedParameterAndStateIsPassedInOnReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var syncedParam = new Synced<Tuple<object, Domain.WorkflowState>>();
        var rAction = functionsRegistry.RegisterAction<object, Domain.WorkflowState>(
            functionTypeId,
            (p, s) =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    flag.Raise();
                    throw new Exception("oh no");
                }
                
                syncedParam.Value = Tuple.Create(p, s);
            }
        );

        await Should.ThrowAsync<Exception>(() =>
            rAction.Invoke(functionInstanceId.Value, "something", new ListState<string>())
        );

        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Param.ShouldBe("something");
        controlPanel.Param = 10;
        (controlPanel.State is ListState<string>).ShouldBeTrue();
        controlPanel.State = new ListState<int>();
        await controlPanel.SaveChanges();
       
        controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.ReInvoke();
        
        var (param, state) = syncedParam.Value!;
        param.ShouldBe(10);
        (state is ListState<int>).ShouldBeTrue();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task StateUpdaterIsCalledBeforeReInvokeOnAction();
    protected async Task StateUpdaterIsCalledBeforeReInvokeOnAction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = functionsRegistry.RegisterAction<string, WorkflowState>(
            functionTypeId,
            inner: (_, state) =>
            {
                state.Value++;
                if (flag.Position == FlagPosition.Lowered)
                    throw new Exception("oh no");
            }
        );

        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId.Value, "something"));
        var state = await store
            .GetFunction(functionId)
            .Map(sf => sf?.State.StateJson.DeserializeFromJsonTo<WorkflowState>());

        state.ShouldNotBeNull();
        state.Value.ShouldBe(1);
        
        flag.Raise();
        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.State.Value = -1;
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();
        await controlPanel.ReInvoke();

        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var s = function.State.StateJson.DeserializeFromJsonTo<WorkflowState>();
        s.Value.ShouldBe(0);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task StateUpdaterIsCalledBeforeReInvokeOnFunc();
    protected async Task StateUpdaterIsCalledBeforeReInvokeOnFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rFunc = functionsRegistry.RegisterFunc<string, WorkflowState, string>(
            functionTypeId,
            inner: (param, state) =>
            {
                state.Value++;
                if (flag.Position == FlagPosition.Lowered)
                    throw new Exception("oh no");

                return param;
            }
        );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId.Value, "something"));
        var sfState = await store
            .GetFunction(functionId)
            .Map(sf => sf?.State.StateJson.DeserializeFromJsonTo<WorkflowState>());

        sfState.ShouldNotBeNull();
        sfState.Value.ShouldBe(1);
        
        flag.Raise();
        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.State.Value = -1;
        await controlPanel.SaveChanges();

        await controlPanel.Refresh();
        var returned = await controlPanel.ReInvoke();
        returned.ShouldBe("something");
        
        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var state = function.State.StateJson.DeserializeFromJsonTo<WorkflowState>();
        state.Value.ShouldBe(0);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    private class WorkflowState : Domain.WorkflowState
    {
        public int Value { get; set; }
    }

    public abstract Task FuncReInvocationSunshineScenario();
    protected async Task FuncReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            functionTypeId,
            async s =>
            {
                await Task.CompletedTask;
                if (flag.Position == FlagPosition.Lowered)
                {
                    flag.Raise();
                    throw new Exception("oh no");
                }
                return s;
            }
        );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId.Value, "something"));

        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.ReInvoke();

        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        function.Result.ResultJson!.DeserializeFromJsonTo<string>().ShouldBe("something");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task FuncWithStateReInvocationSunshineScenario();
    protected async Task FuncWithStateReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rFunc = functionsRegistry.RegisterFunc<string, ListState<string>, string>(
            functionTypeId,
            async (param, state) =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    state.List.Add("hello");
                    await state.Save();
                    flag.Raise();
                    throw new Exception("oh no");
                }

                state.List.Add("world");
                return param;
            }
        );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId.Value, "something"));

        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.State.List.Clear();
        await controlPanel.SaveChanges();
        
        controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var result = await controlPanel.ReInvoke(); 
        result.ShouldBe("something");

        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        function.Result.ResultJson!.DeserializeFromJsonTo<string>().ShouldBe("something");
        var state = function.State.StateJson.DeserializeFromJsonTo<ListState<string>>();
        state.List.Single().ShouldBe("world");

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ReInvocationFailsWhenTheFunctionDoesNotExist();
    protected async Task ReInvocationFailsWhenTheFunctionDoesNotExist(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            (string _) => {}
        );

        await rAction.Invoke(functionInstanceId.Value, "");
        var controlPanel1 = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        var controlPanel2 = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel1.Delete();
        
        await Should.ThrowAsync<UnexpectedFunctionState>(() => controlPanel2.ReInvoke());

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
}
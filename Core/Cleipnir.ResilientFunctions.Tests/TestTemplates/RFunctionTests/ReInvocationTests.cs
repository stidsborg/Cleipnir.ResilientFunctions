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
                enableWatchdogs: false
            )
        );
        var syncedParameter = new Synced<string>();

        var rFunc = functionsRegistry
            .RegisterAction(
                functionType, Task (string s) =>
                {
                    if (flag.Position == FlagPosition.Lowered)
                    {
                        flag.Raise();
                        throw new Exception("oh no");
                    }

                    syncedParameter.Value = s;
                    return Task.CompletedTask;
                }
            );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke("something", "something"));

        await rFunc.ControlPanel("something").Result!.Restart();
        
        syncedParameter.Value.ShouldBe("something");

        var function = await store.GetFunction(new FlowId(functionType, "something"));
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task ActionWithStateReInvocationSunshineScenario();
    protected async Task ActionWithStateReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );

        var rAction = functionsRegistry.RegisterAction<string>(
            flowType,
            async (param, workflow) =>
            {
                var state = await workflow.States.CreateOrGet<ListState<string>>("State");
                if (flag.Position == FlagPosition.Lowered)
                {
                    state.List.Add("hello");
                    await state.Save();
                    flag.Raise();
                    throw new Exception("oh no");
                }
                state.List.Add("world");
                await state.Save();
            }
        );

        await Should.ThrowAsync<Exception>(() =>
            rAction.Invoke(flowInstance.Value, "something")
        );

        var syncedListFromState = new Synced<List<string>>();
        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
            
        syncedListFromState.Value = new List<string>((await controlPanel.States.Get<ListState<string>>("State")).List);
        await controlPanel.States.Set("State", new ListState<string>());
        await controlPanel.SaveChanges();
        
        controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.Restart();
        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        await controlPanel.Refresh();
        var state = await controlPanel.States.Get<ListState<string>>("State");
        state.List.Single().ShouldBe("world");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario();
    protected async Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );

        var syncedParam = new Synced<string>();
        var rAction = functionsRegistry.RegisterAction<string>(
            flowType,
            param =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    flag.Raise();
                    throw new Exception("oh no");
                }
                
                syncedParam.Value = param;
                return Task.CompletedTask;
            }
        );

        await Should.ThrowAsync<Exception>(() =>
            rAction.Invoke(flowInstance.Value, "something")
        );
        
        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Param.ShouldBe("something");
        controlPanel.Param = "something_else";
        await controlPanel.SaveChanges();
       
        controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.Restart();
        
        syncedParam.Value.ShouldBe("something_else");
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task UpdatedParameterAndStateIsPassedInOnReInvocationSunshineScenario();
    protected async Task UpdatedParameterAndStateIsPassedInOnReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );

        var syncedValue = new Synced<string>();
        var rAction = functionsRegistry.RegisterAction<string>(
            flowType,
            (param, workflow) =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    flag.Raise();
                    throw new Exception("oh no");
                }
                
                syncedValue.Value = param;
                return Task.CompletedTask;
            }
        );

        await Should.ThrowAsync<Exception>(() =>
            rAction.Invoke(flowInstance.Value, "something")
        );

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Param.ShouldBe("something");
        controlPanel.Param = "something_else";
        await controlPanel.SaveChanges();
       
        controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.Restart();
        
        syncedValue.Value.ShouldBe("something_else");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task FuncReInvocationSunshineScenario();
    protected async Task FuncReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );

        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            flowType,
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

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(flowInstance.Value, "something"));

        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.Restart();

        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        function.Result!.DeserializeFromJsonTo<string>().ShouldBe("something");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task FuncWithStateReInvocationSunshineScenario();
    protected async Task FuncWithStateReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );
        
        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            flowType,
            async (param, workflow) =>
            {
                var state = await workflow.States.CreateOrGet<ListState<string>>("State");
                if (flag.Position == FlagPosition.Lowered)
                {
                    state.List.Add("hello");
                    await state.Save();
                    flag.Raise();
                    throw new Exception("oh no");
                }

                state.List.Add("world");
                await state.Save();
                return param;
            }
        );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(flowInstance.Value, "something"));

        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.States.Remove("State");
        await controlPanel.SaveChanges();
        
        controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var result = await controlPanel.Restart(); 
        result.ShouldBe("something");

        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        function.Result!.DeserializeFromJsonTo<string>().ShouldBe("something");
        await controlPanel.Refresh();
        var state = await controlPanel.States.Get<ListState<string>>("State");
        state!.List.Single().ShouldBe("world");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task ReInvocationFailsWhenTheFunctionDoesNotExist();
    protected async Task ReInvocationFailsWhenTheFunctionDoesNotExist(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );

        var rAction = functionsRegistry.RegisterAction(
            flowType,
            (string _) => Task.CompletedTask
        );

        await rAction.Invoke(flowInstance.Value, "");
        var controlPanel1 = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var controlPanel2 = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel1.Delete();
        
        await Should.ThrowAsync<UnexpectedStateException>(() => controlPanel2.Restart());

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
}
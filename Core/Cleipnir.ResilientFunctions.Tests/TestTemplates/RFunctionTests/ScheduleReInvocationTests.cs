using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class ScheduleReInvocationTests
{
    public abstract Task ActionReInvocationSunshineScenario();
    protected async Task ActionReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionType = TestFunctionId.Create().TypeId;
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
                functionType,
                inner: async (string s) =>
                {
                    await Task.CompletedTask;
                    if (flag.Position == FlagPosition.Lowered)
                    {
                        flag.Raise();
                        throw new Exception("oh no");
                    }

                    syncedParameter.Value = s;
                }
            );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke("something", "something"));

        await rFunc.ControlPanel("something").Result!.ScheduleReInvoke();

        var functionId = new FunctionId(functionType, "something");
        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );
        
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

        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId.Value, "something"));

        var syncedListFromState = new Synced<List<string>>();
        var controlPanel = await rAction.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        syncedListFromState.Value = new List<string>(controlPanel.State.List);
        controlPanel.State.List.Clear();
        await controlPanel.SaveChanges();
        
        await rAction.ControlPanel(functionInstanceId).Result!.ScheduleReInvoke();
        
        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );
        
        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var state = function.State.StateJson.DeserializeFromJsonTo<ListState<string>>();
        state.List.Single().ShouldBe("world");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
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
        await controlPanel.ScheduleReInvoke();

        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );

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

        var stateList = new Synced<List<string>>();
        var controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        stateList.Value = new List<string>(controlPanel.State.List);
        controlPanel.State.List.Clear();
        await controlPanel.SaveChanges();

        controlPanel = await rFunc.ControlPanel(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.ScheduleReInvoke();
        
        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );
        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        function.Result.ResultJson!.DeserializeFromJsonTo<string>().ShouldBe("something");
        var state = function.State.StateJson.DeserializeFromJsonTo<ListState<string>>();
        state.List.Single().ShouldBe("world");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
}
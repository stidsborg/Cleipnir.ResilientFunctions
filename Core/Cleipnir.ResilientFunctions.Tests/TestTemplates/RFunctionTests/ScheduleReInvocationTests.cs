using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
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
        const string functionType = "someFunctionType";
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );
        var syncedParameter = new Synced<string>();

        var rFunc = rFunctions
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

        await rFunc.ScheduleReInvoke(functionInstanceId: "something", expectedEpoch: 0);

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

    public abstract Task ActionWithScrapbookReInvocationSunshineScenario();
    protected async Task ActionWithScrapbookReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rAction = rFunctions.RegisterAction<string, ListScrapbook<string>>(
            functionType,
            async (param, scrapbook) =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    scrapbook.List.Add("hello");
                    await scrapbook.Save();
                    flag.Raise();
                    throw new Exception("oh no");
                }
                scrapbook.List.Add("world");
            }
        );

        await Should.ThrowAsync<Exception>(() => rAction.Invoke("something", "something"));

        var syncedListFromScrapbook = new Synced<List<string>>();
        var controlPanel = await rAction.ControlPanel.For(functionInstanceId: "something").ShouldNotBeNullAsync();
        syncedListFromScrapbook.Value = new List<string>(controlPanel.Scrapbook.List);
        controlPanel.Scrapbook.List.Clear();
        await controlPanel.SaveParameterAndScrapbook().ShouldBeTrueAsync();

        controlPanel = await rAction.ControlPanel.For(functionInstanceId: "something");
        await rAction.ScheduleReInvoke(functionInstanceId: "something", expectedEpoch: controlPanel!.Epoch); 
        
        var functionId = new FunctionId(functionType, "something");
        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );
        
        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var scrapbook = function.Scrapbook.ScrapbookJson.DeserializeFromJsonTo<ListScrapbook<string>>();
        scrapbook.List.Single().ShouldBe("world");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task FuncReInvocationSunshineScenario();
    protected async Task FuncReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionType,
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

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke("something", "something"));

        await rFunc.ScheduleReInvoke(functionInstanceId: "something", expectedEpoch: 0);

        var functionId = new FunctionId(functionType, "something");
        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );

        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        function.Result.ResultJson!.DeserializeFromJsonTo<string>().ShouldBe("something");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task FuncWithScrapbookReInvocationSunshineScenario();
    protected async Task FuncWithScrapbookReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rFunc = rFunctions.RegisterFunc<string, ListScrapbook<string>, string>(
            functionType,
            async (param, scrapbook) =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    scrapbook.List.Add("hello");
                    await scrapbook.Save();
                    flag.Raise();
                    throw new Exception("oh no");
                }
                scrapbook.List.Add("world");
                return param;
            }
        );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke("something", "something"));

        var scrapbookList = new Synced<List<string>>();
        var controlPanel = await rFunc.ControlPanel.For(functionInstanceId: "something").ShouldNotBeNullAsync();
        scrapbookList.Value = new List<string>(controlPanel.Scrapbook.List);
        controlPanel.Scrapbook.List.Clear();
        await controlPanel.SaveParameterAndScrapbook();

        controlPanel = await rFunc.ControlPanel.For(functionInstanceId: "something");
        await rFunc.ScheduleReInvoke(functionInstanceId: "something", controlPanel!.Epoch);

        var functionId = new FunctionId(functionType, "something");
        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );
        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        function.Result.ResultJson!.DeserializeFromJsonTo<string>().ShouldBe("something");
        var scrapbook = function.Scrapbook.ScrapbookJson.DeserializeFromJsonTo<ListScrapbook<string>>();
        scrapbook.List.Single().ShouldBe("world");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ReInvocationSucceedsDespiteUnexpectedStatusWhenNotThrowOnUnexpectedFunctionState();
    protected async Task ReInvocationSucceedsDespiteUnexpectedStatusWhenNotThrowOnUnexpectedFunctionState(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rFunc = rFunctions
            .RegisterAction(
                functionType,
                (string _) => Task.FromException(new Exception("oh no"))
            );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke("something", "something"));

        await Should.ThrowAsync<UnexpectedFunctionState>(
            () => rFunc.ScheduleReInvoke(functionInstanceId: "something", expectedEpoch: 1)
        );

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ReInvocationFailsWhenTheFunctionDoesNotExist();
    protected async Task ReInvocationFailsWhenTheFunctionDoesNotExist(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rFunc = rFunctions
            .RegisterAction(
                functionType,
                (string _) => Task.FromException(new Exception("oh no"))
            );

        await Should.ThrowAsync<UnexpectedFunctionState>(() =>
            rFunc.ScheduleReInvoke(functionInstanceId: "something", expectedEpoch: 0)
        );

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
}
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

        await rFunc.ControlPanels.For("something").Result!.ReInvoke();
        
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
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
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
            functionTypeId,
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

        await Should.ThrowAsync<Exception>(() =>
            rAction.Invoke(functionInstanceId.Value, "something")
        );

        var syncedListFromScrapbook = new Synced<List<string>>();
        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
            
        syncedListFromScrapbook.Value = new List<string>(controlPanel.Scrapbook.List);
        controlPanel.Scrapbook.List.Clear();
        await controlPanel.SaveChanges();
        
        controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.ReInvoke();
        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var scrapbook = function.Scrapbook.ScrapbookJson.DeserializeFromJsonTo<ListScrapbook<string>>();
        scrapbook.List.Single().ShouldBe("world");
        
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
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var syncedParam = new Synced<object>();
        var rAction = rFunctions.RegisterAction<object>(
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
        
        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Param.ShouldBe("something");
        controlPanel.Param = 10;
        await controlPanel.SaveChanges();
       
        controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.ReInvoke();
        
        syncedParam.Value.ShouldBe(10);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task UpdatedParameterAndScrapbookIsPassedInOnReInvocationSunshineScenario();
    protected async Task UpdatedParameterAndScrapbookIsPassedInOnReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
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

        var syncedParam = new Synced<Tuple<object, RScrapbook>>();
        var rAction = rFunctions.RegisterAction<object, RScrapbook>(
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
            rAction.Invoke(functionInstanceId.Value, "something", new ListScrapbook<string>())
        );

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Param.ShouldBe("something");
        controlPanel.Param = 10;
        (controlPanel.Scrapbook is ListScrapbook<string>).ShouldBeTrue();
        controlPanel.Scrapbook = new ListScrapbook<int>();
        await controlPanel.SaveChanges();
       
        controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel.ReInvoke();
        
        var (param, scrapbook) = syncedParam.Value!;
        param.ShouldBe(10);
        (scrapbook is ListScrapbook<int>).ShouldBeTrue();
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ScrapbookUpdaterIsCalledBeforeReInvokeOnAction();
    protected async Task ScrapbookUpdaterIsCalledBeforeReInvokeOnAction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = rFunctions.RegisterAction<string, Scrapbook>(
            functionTypeId,
            inner: (_, scrapbook) =>
            {
                scrapbook.Value++;
                if (flag.Position == FlagPosition.Lowered)
                    throw new Exception("oh no");
            }
        );

        await Should.ThrowAsync<Exception>(() => rAction.Invoke(functionInstanceId.Value, "something"));
        var sfScrapbook = await store
            .GetFunction(functionId)
            .Map(sf => sf?.Scrapbook.ScrapbookJson.DeserializeFromJsonTo<Scrapbook>());

        sfScrapbook.ShouldNotBeNull();
        sfScrapbook.Value.ShouldBe(1);
        
        flag.Raise();
        var controlPanel = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Scrapbook.Value = -1;
        await controlPanel.SaveChanges();
        await controlPanel.Refresh();
        await controlPanel.ReInvoke();

        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var scrapbook = function.Scrapbook.ScrapbookJson.DeserializeFromJsonTo<Scrapbook>();
        scrapbook.Value.ShouldBe(0);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc();
    protected async Task ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rFunc = rFunctions.RegisterFunc<string, Scrapbook, string>(
            functionTypeId,
            inner: (param, scrapbook) =>
            {
                scrapbook.Value++;
                if (flag.Position == FlagPosition.Lowered)
                    throw new Exception("oh no");

                return param;
            }
        );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId.Value, "something"));
        var sfScrapbook = await store
            .GetFunction(functionId)
            .Map(sf => sf?.Scrapbook.ScrapbookJson.DeserializeFromJsonTo<Scrapbook>());

        sfScrapbook.ShouldNotBeNull();
        sfScrapbook.Value.ShouldBe(1);
        
        flag.Raise();
        var controlPanel = await rFunc.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Scrapbook.Value = -1;
        await controlPanel.SaveChanges();
        
        var returned = await rFunc.ReInvoke(functionInstanceId.Value, expectedEpoch: 1);
        returned.ShouldBe("something");
        
        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var scrapbook = function.Scrapbook.ScrapbookJson.DeserializeFromJsonTo<Scrapbook>();
        scrapbook.Value.ShouldBe(0);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    private class Scrapbook : RScrapbook
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
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rFunc = rFunctions.RegisterFunc<string, string>(
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

        await rFunc.ReInvoke(functionInstanceId.Value, expectedEpoch: 0);

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
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
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
            functionTypeId,
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

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(functionInstanceId.Value, "something"));

        var controlPanel = await rFunc.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        controlPanel.Scrapbook.List.Clear();
        await controlPanel.SaveChanges();
        
        controlPanel = await rFunc.ControlPanel.For(functionInstanceId).ShouldNotBeNullAsync();
        var result = await rFunc.ReInvoke(functionInstanceId.Value, expectedEpoch: controlPanel.Epoch); 
        result.ShouldBe("something");

        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        function.Result.ResultJson!.DeserializeFromJsonTo<string>().ShouldBe("something");
        var scrapbook = function.Scrapbook.ScrapbookJson.DeserializeFromJsonTo<ListScrapbook<string>>();
        scrapbook.List.Single().ShouldBe("world");

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ReInvocationFailsWhenTheFunctionDoesNotExist();
    protected async Task ReInvocationFailsWhenTheFunctionDoesNotExist(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            (string _) => {}
        );

        await rAction.Invoke(functionInstanceId.Value, "");
        var controlPanel1 = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        var controlPanel2 = await rAction.ControlPanels.For(functionInstanceId).ShouldNotBeNullAsync();
        await controlPanel1.Delete();
        
        await Should.ThrowAsync<UnexpectedFunctionState>(() => controlPanel2.ReInvoke());

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ReInvocationThroughRFunctionsSunshine();
    protected async Task ReInvocationThroughRFunctionsSunshine(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        const string functionInstance = "someFunctionInstance";
        var functionId = new FunctionId(functionType, functionInstance);
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var flag = new SyncedFlag();
        var rAction = rFunctions.RegisterAction(
            functionType,
            (string _) =>
            {
                if (flag.IsRaised)
                    return Result.Succeed;
                
                flag.Raise();
                return Postpone.For(10_000);
            }).Invoke;

        await rAction(functionInstance, param: "").ShouldThrowAsync<Exception>();

        await rFunctions.ReInvoke(functionType, functionInstance, expectedEpoch: 0);

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ScheduleReInvocationThroughRFunctionsSunshine();
    protected async Task ScheduleReInvocationThroughRFunctionsSunshine(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            )
        );

        var flag = new SyncedFlag();
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            (string _) =>
            {
                if (flag.IsRaised)
                    return Result.Succeed;
                
                flag.Raise();
                return Postpone.For(10_000);
            }).Invoke;

        await rAction(functionInstanceId.Value, param: "").ShouldThrowAsync<Exception>();

        await rFunctions.ScheduleReInvoke(functionTypeId.Value, functionInstanceId.Value, expectedEpoch: 0);

        await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded));

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
}
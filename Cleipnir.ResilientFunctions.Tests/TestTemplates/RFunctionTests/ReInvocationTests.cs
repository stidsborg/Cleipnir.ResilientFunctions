using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils.Scrapbooks;
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
        using var rFunctions = RFunctions.Create(
            store,
            unhandledExceptionCatcher.Catch,
            crashedCheckFrequency: TimeSpan.Zero,
            postponedCheckFrequency: TimeSpan.Zero
        );
        var syncedParameter = new Synced<string>();

        var rFunc = rFunctions.Register<string>(
            functionType,
            async s =>
            {
                await Task.CompletedTask;
                if (flag.Position == FlagPosition.Lowered)
                {
                    flag.Raise();
                    return Fail.WithException(new Exception("oh no"));
                }

                syncedParameter.Value = s;
                return RResult.Success;
            },
            _ => _
        );

        var result = await rFunc.Invoke("something");
        result.FailedException.ShouldNotBeNull();
        result.FailedException.ShouldBeOfType<Exception>();
        
        result = await rFunc.ReInvoke(
            "something",
            new[] {Status.Failed}
        );

        result.EnsureSuccess();
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
        using var rFunctions = RFunctions.Create(
            store,
            unhandledExceptionCatcher.Catch,
            crashedCheckFrequency: TimeSpan.Zero,
            postponedCheckFrequency: TimeSpan.Zero
        );

        var rAction = rFunctions.Register<string, ListScrapbook<string>>(
            functionType,
            async (param, scrapbook) =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    scrapbook.List.Add("hello");
                    await scrapbook.Save();
                    flag.Raise();
                    return Fail.WithException(new Exception("oh no"));
                }
                scrapbook.List.Add("world");
                return RResult.Success;
            },
            _ => _
        );

        var result = await rAction.Invoke("something");
        result.FailedException.ShouldNotBeNull();
        result.FailedException.ShouldBeOfType<Exception>();
        
        var syncedListFromScrapbook = new Synced<List<string>>();
        await store.UpdateScrapbook<ListScrapbook<string>>(
            new FunctionId(functionType, "something"),
            s =>
            {
                syncedListFromScrapbook.Value = new List<string>(s.List);
                s.List.Clear();
            },
            expectedStatuses: new [] {Status.Failed}
        );
        
        result = await rAction.ReInvoke(
            "something",
            new[] {Status.Failed}
        );

        result.EnsureSuccess();

        var function = await store.GetFunction(new FunctionId(functionType, "something"));
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var scrapbook = function.Scrapbook!.ScrapbookJson!.DeserializeFromJsonTo<ListScrapbook<string>>();
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
        using var rFunctions = RFunctions.Create(
            store,
            unhandledExceptionCatcher.Catch,
            crashedCheckFrequency: TimeSpan.Zero,
            postponedCheckFrequency: TimeSpan.Zero
        );

        var rFunc = rFunctions.Register<string, string>(
            functionType,
            async s =>
            {
                await Task.CompletedTask;
                if (flag.Position == FlagPosition.Lowered)
                {
                    flag.Raise();
                    return Fail.WithException(new Exception("oh no"));
                }
                return s;
            },
            _ => _
        );

        var result = await rFunc.Invoke("something");
        result.FailedException.ShouldNotBeNull();
        result.FailedException.ShouldBeOfType<Exception>();
        
        result = await rFunc.ReInvoke(
            "something",
            new[] {Status.Failed}
        );

        result.EnsureSuccess().ShouldBe("something");

        var function = await store.GetFunction(new FunctionId(functionType, "something"));
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        function.Result!.ResultJson!.DeserializeFromJsonTo<string>().ShouldBe("something");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task FuncWithScrapbookReInvocationSunshineScenario();
    protected async Task FuncWithScrapbookReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = RFunctions.Create(
            store,
            unhandledExceptionCatcher.Catch,
            crashedCheckFrequency: TimeSpan.Zero,
            postponedCheckFrequency: TimeSpan.Zero
        );

        var rFunc = rFunctions.Register<string, ListScrapbook<string>, string>(
            functionType,
            async (param, scrapbook) =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    scrapbook.List.Add("hello");
                    await scrapbook.Save();
                    flag.Raise();
                    return Fail.WithException(new Exception("oh no"));
                }
                scrapbook.List.Add("world");
                return param;
            },
            _ => _
        );

        var result = await rFunc.Invoke("something");
        result.FailedException.ShouldNotBeNull();
        result.FailedException.ShouldBeOfType<Exception>();
        
        var scrapbookList = new Synced<List<string>>();

        await store.UpdateScrapbook<ListScrapbook<string>>(
            new FunctionId(functionType, "something"),
            s =>
            {
                scrapbookList.Value = new List<string>(s.List);
                s.List.Clear();
            },
            new [] {Status.Failed}
        );
            
        result = await rFunc.ReInvoke(
            "something",
            new[] {Status.Failed}
        );

        result.EnsureSuccess().ShouldBe("something");

        var function = await store.GetFunction(new FunctionId(functionType, "something"));
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        function.Result!.ResultJson!.DeserializeFromJsonTo<string>().ShouldBe("something");
        var scrapbook = function.Scrapbook!.ScrapbookJson!.DeserializeFromJsonTo<ListScrapbook<string>>();
        scrapbook.List.Single().ShouldBe("world");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ReInvocationFailsWhenItHasUnexpectedStatus();
    protected async Task ReInvocationFailsWhenItHasUnexpectedStatus(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = RFunctions.Create(
            store,
            unhandledExceptionCatcher.Catch,
            crashedCheckFrequency: TimeSpan.Zero,
            postponedCheckFrequency: TimeSpan.Zero
        );

        var rFunc = rFunctions.Register<string>(
            functionType,
            _ => new Exception("oh no").ToFailedRResult().ToTask(),
            _ => _
        );
        
        await rFunc.Invoke("something");

        await Should.ThrowAsync<FunctionInvocationException>(() =>
            rFunc.ReInvoke("something", new[] {Status.Executing})
        );

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ReInvocationFailsWhenTheFunctionDoesNotExist();
    protected async Task ReInvocationFailsWhenTheFunctionDoesNotExist(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = RFunctions.Create(
            store,
            unhandledExceptionCatcher.Catch,
            crashedCheckFrequency: TimeSpan.Zero,
            postponedCheckFrequency: TimeSpan.Zero
        );

        var rFunc = rFunctions.Register<string>(
            functionType,
            _ => new Exception("oh no").ToFailedRResult().ToTask(),
            _ => _
        );

        await Should.ThrowAsync<FunctionInvocationException>(() =>
            rFunc.ReInvoke("something", new[] {Status.Executing})
        );

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
}
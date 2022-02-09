using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;
using static Cleipnir.ResilientFunctions.Tests.Utils.FlagPosition;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class PersistedCallbackTests
{
    public abstract Task PersistedCallbackIsInvokedAfterFuncStateHasBeenPersisted();
    protected async Task PersistedCallbackIsInvokedAfterFuncStateHasBeenPersisted(IFunctionStore store)
    {
        var functionTypeId = nameof(PersistedCallbackIsInvokedAfterFuncStateHasBeenPersisted).ToFunctionTypeId();
        const string functionInstanceId = "someFunctionId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = RFunctions.Create(store, unhandledExceptionCatcher.Catch);
        var rFunc = rFunctions.Register(
            functionTypeId,
            (string _) => NeverCompletingTask.OfType<RResult<string>>(),
            _ => _
        );

        var flag = new SyncedFlag();
        _ = rFunc(functionInstanceId, flag.Raise);

        await BusyWait.UntilAsync(() => flag.Position == Raised);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Parameter.DefaultDeserialize().ShouldBe(functionInstanceId);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task PersistedCallbackIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted();
    protected async Task PersistedCallbackIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted(IFunctionStore store)
    {
        var functionTypeId = nameof(PersistedCallbackIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted).ToFunctionTypeId();
        const string functionInstanceId = "someFunctionId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = RFunctions.Create(store, unhandledExceptionCatcher.Catch);
        var rFunc = rFunctions.Register(
            functionTypeId,
            (string _, Scrapbook _) => NeverCompletingTask.OfType<RResult<string>>(),
            _ => _
        );

        var flag = new SyncedFlag();
        _ = rFunc(functionInstanceId, flag.Raise);

        await BusyWait.UntilAsync(() => flag.Position == Raised);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Scrapbook.ShouldNotBeNull();
        storedFunction.Scrapbook.ScrapbookType.ResolveType().ShouldBe(typeof(Scrapbook));
        storedFunction.Parameter.DefaultDeserialize().ShouldBe(functionInstanceId);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task PersistedCallbackIsInvokedAfterActionWithScrapbookStateHasBeenPersisted();
    protected async Task PersistedCallbackIsInvokedAfterActionWithScrapbookStateHasBeenPersisted(IFunctionStore store)
    {
        var functionTypeId = nameof(PersistedCallbackIsInvokedAfterActionWithScrapbookStateHasBeenPersisted).ToFunctionTypeId();
        const string functionInstanceId = "someFunctionId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = RFunctions.Create(store, unhandledExceptionCatcher.Catch);
        var rAction = rFunctions.Register(
            functionTypeId,
            (string _, Scrapbook _) => NeverCompletingTask.OfType<RResult>(),
            _ => _
        ).RAction;

        var flag = new SyncedFlag();
        _ = rAction(functionInstanceId, flag.Raise);

        await BusyWait.UntilAsync(() => flag.Position == Raised);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Scrapbook.ShouldNotBeNull();
        storedFunction.Scrapbook.ScrapbookType.ResolveType().ShouldBe(typeof(Scrapbook));
        storedFunction.Parameter.DefaultDeserialize().ShouldBe(functionInstanceId);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task PersistedCallbackIsInvokedAfterActionStateHasBeenPersisted();
    protected async Task PersistedCallbackIsInvokedAfterActionStateHasBeenPersisted(IFunctionStore store)
    {
        var functionTypeId = nameof(PersistedCallbackIsInvokedAfterActionStateHasBeenPersisted).ToFunctionTypeId();
        const string functionInstanceId = "someFunctionId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = RFunctions.Create(store, unhandledExceptionCatcher.Catch);
        var rFunc = rFunctions.Register(
            functionTypeId,
            (string _) => NeverCompletingTask.OfType<RResult>(),
            _ => _
        ).RAction;

        var flag = new SyncedFlag();
        _ = rFunc(functionInstanceId, flag.Raise);

        await BusyWait.UntilAsync(() => flag.Position == Raised);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Parameter.DefaultDeserialize().ShouldBe(functionInstanceId);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    private class Scrapbook : RScrapbook {}
}
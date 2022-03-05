using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class ScheduledInvocationTests
{
    public abstract Task ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted();
    protected async Task ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted).ToFunctionTypeId();
        const string functionInstanceId = "someFunctionId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(store, unhandledExceptionCatcher.Catch);
        var schedule = rFunctions.Register(
            functionTypeId,
            (string _) => NeverCompletingTask.OfType<Return<string>>()
        ).Schedule;
        
        await schedule(functionInstanceId, functionInstanceId);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Parameter.DefaultDeserialize().ShouldBe(functionInstanceId);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ScheduledFunctionIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted();
    protected async Task ScheduledFunctionIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ScheduledFunctionIsInvokedAfterFuncWithScrapbookStateHasBeenPersisted).ToFunctionTypeId();
        const string functionInstanceId = "someFunctionId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(store, unhandledExceptionCatcher.Catch);
        var schedule = rFunctions.Register(
            functionTypeId,
            (string _, Scrapbook _) => NeverCompletingTask.OfType<Return<string>>()
        ).Schedule;

        await schedule(functionInstanceId, functionInstanceId);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Scrapbook.ShouldNotBeNull();
        storedFunction.Scrapbook.ScrapbookType.ResolveType().ShouldBe(typeof(Scrapbook));
        storedFunction.Parameter.DefaultDeserialize().ShouldBe(functionInstanceId);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ScheduledFunctionIsInvokedAfterActionWithScrapbookStateHasBeenPersisted();
    protected async Task ScheduledFunctionIsInvokedAfterActionWithScrapbookStateHasBeenPersisted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ScheduledFunctionIsInvokedAfterActionWithScrapbookStateHasBeenPersisted).ToFunctionTypeId();
        const string functionInstanceId = "someFunctionId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(store, unhandledExceptionCatcher.Catch);
        var schedule = rFunctions.Register(
            functionTypeId,
            (string _, Scrapbook _) => NeverCompletingTask.OfType<Return>()
        ).Schedule;

        await schedule(functionInstanceId, functionInstanceId);
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Scrapbook.ShouldNotBeNull();
        storedFunction.Scrapbook.ScrapbookType.ResolveType().ShouldBe(typeof(Scrapbook));
        storedFunction.Parameter.DefaultDeserialize().ShouldBe(functionInstanceId);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted();
    protected async Task ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted).ToFunctionTypeId();
        const string functionInstanceId = "someFunctionId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(store, unhandledExceptionCatcher.Catch);
        var schedule = rFunctions.Register(
            functionTypeId,
            (string _) => NeverCompletingTask.OfType<Return>()
        ).Schedule;

        await schedule(functionInstanceId, functionInstanceId);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Parameter.DefaultDeserialize().ShouldBe(functionInstanceId);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    private class Scrapbook : RScrapbook {}
}
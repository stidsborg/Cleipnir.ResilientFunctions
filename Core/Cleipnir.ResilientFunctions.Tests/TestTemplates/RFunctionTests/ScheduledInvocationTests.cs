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
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var schedule = functionsRegistry.RegisterFunc(
            functionTypeId,
            (string _) => NeverCompletingTask.OfType<Result<string>>()
        ).Schedule;
        
        await schedule(functionInstanceId, functionInstanceId);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Parameter.DefaultDeserialize().ShouldBe(functionInstanceId);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted();
    protected async Task ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted).ToFunctionTypeId();
        const string functionInstanceId = "someFunctionId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var schedule = functionsRegistry.RegisterFunc(
            functionTypeId,
            (string _) => NeverCompletingTask.OfType<Result<string>>()
        ).Schedule;

        await schedule(functionInstanceId, functionInstanceId);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Parameter.DefaultDeserialize().ShouldBe(functionInstanceId);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted();
    protected async Task ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted).ToFunctionTypeId();
        const string functionInstanceId = "someFunctionId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var schedule = functionsRegistry.RegisterAction(
            functionTypeId,
            (string _) => NeverCompletingTask.OfType<Result>()
        ).Schedule;

        await schedule(functionInstanceId, functionInstanceId);
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
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
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var schedule = functionsRegistry.RegisterFunc(
            functionTypeId,
            (string _) => NeverCompletingTask.OfType<Result>()
        ).Schedule;

        await schedule(functionInstanceId, functionInstanceId);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Parameter.DefaultDeserialize().ShouldBe(functionInstanceId);
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
}
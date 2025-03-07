using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class ScheduledInvocationTests
{
    public abstract Task ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted();
    protected async Task ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted).ToFlowType();
        const string flowInstance = "someFunctionId";
        var functionId = new FlowId(flowType, flowInstance);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var reg = functionsRegistry.RegisterFunc(
            flowType,
            (string _) => NeverCompletingTask.OfType<Result<string>>()
        );
        var schedule = reg.Schedule;
        
        await schedule(flowInstance, flowInstance);

        var storedFunction = await store.GetFunction(reg.MapToStoredId(functionId.Instance));
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Parameter!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe(flowInstance);
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted();
    protected async Task ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted).ToFlowType();
        const string flowInstance = "someFunctionId";
        var functionId = new FlowId(flowType, flowInstance);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var reg = functionsRegistry.RegisterFunc(
                flowType,
                (string _) => NeverCompletingTask.OfType<Result<string>>()
            );
        var schedule = reg.Schedule;

        await schedule(flowInstance, flowInstance);

        var storedFunction = await store.GetFunction(reg.MapToStoredId(functionId.Instance));
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Parameter!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe(flowInstance);
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted();
    protected async Task ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted).ToFlowType();
        const string flowInstance = "someFunctionId";
        var functionId = new FlowId(flowType, flowInstance);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var reg = functionsRegistry.RegisterAction(
            flowType,
            (string _) => NeverCompletingTask.OfType<Result<Unit>>()
        );
        var schedule = reg.Schedule;

        await schedule(flowInstance, flowInstance);
        
        var storedFunction = await store.GetFunction(reg.MapToStoredId(functionId.Instance));
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Parameter!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe(flowInstance);
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted();
    protected async Task ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted).ToFlowType();
        const string flowInstance = "someFunctionId";
        var functionId = new FlowId(flowType, flowInstance);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        var reg = functionsRegistry.RegisterFunc(
            flowType,
            (string _) => NeverCompletingTask.OfType<Result<Unit>>()
        );
        var schedule = reg.Schedule;

        await schedule(flowInstance, flowInstance);

        var storedFunction = await store.GetFunction(reg.MapToStoredId(functionId.Instance));
        storedFunction.ShouldNotBeNull();
        
        storedFunction.Status.ShouldBe(Status.Executing);
        storedFunction.Parameter!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe(flowInstance);
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
}
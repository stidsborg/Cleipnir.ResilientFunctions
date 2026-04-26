using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class RoutingTests
{
    public abstract Task MessageIsRoutedToParamlessInstance();
    protected async Task MessageIsRoutedToParamlessInstance(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store, 
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var syncedFlag = new SyncedFlag();
        var syncedValue = new Synced<string>();
        
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async workflow =>
            {
                var someMessage = await workflow.Message<SomeMessage>();
                syncedValue.Value = someMessage.Value;
                syncedFlag.Raise();
            }
        );

        await registration.Schedule(flowInstance);

        await registration.SendMessage(
            flowInstance,
            new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!")
        );
        
        await syncedFlag.WaitForRaised();
        syncedValue.Value.ShouldBe("SomeValue!");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task MessageIsRoutedToActionInstance();
    protected async Task MessageIsRoutedToActionInstance(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store, 
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var syncedFlag = new SyncedFlag();
        var syncedValue = new Synced<string>();
        
        var registration = functionsRegistry.RegisterAction(
            flowType,
            inner: async (string _, Workflow workflow) =>
            {
                var someMessage = await workflow.Message<SomeMessage>();
                syncedValue.Value = someMessage.Value;
                syncedFlag.Raise();
            }
        );

        await registration.Schedule(flowInstance.Value, param: "SomeParam");

        await registration.SendMessage(
            flowInstance,
            new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!")
        );
        
        await syncedFlag.WaitForRaised();
        syncedValue.Value.ShouldBe("SomeValue!");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task MessageIsRoutedToFuncInstance();
    protected async Task MessageIsRoutedToFuncInstance(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store, 
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var syncedFlag = new SyncedFlag();
        var syncedValue = new Synced<string>();
        
        var registration = functionsRegistry.RegisterFunc(
            flowType,
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                var someMessage = await workflow.Message<SomeMessage>();
                syncedValue.Value = someMessage.Value;
                syncedFlag.Raise();
                
                return "SomeResult";
            }
        );

        await registration.Schedule(flowInstance.Value, param: "SomeParam");

        await registration.SendMessage(
            flowInstance,
            new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!")
        );
        
        await syncedFlag.WaitForRaised();
        syncedValue.Value.ShouldBe("SomeValue!");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task MessageIsRoutedUsingRoutingInfo();
    protected async Task MessageIsRoutedUsingRoutingInfo(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store, 
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var syncedFlag = new SyncedFlag();
        var syncedValue = new Synced<string>();
        
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async workflow =>
            {
                var someMessage = await workflow.Message<SomeMessage>();
                syncedValue.Value = someMessage.Value;
                syncedFlag.Raise();
            }
        );

        await registration.SendMessage(
            flowInstance,
            new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!")
        );
        
        await syncedFlag.WaitForRaised(2_000);
        syncedValue.Value.ShouldBe("SomeValue!");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    #region Paramless is started by message

    public abstract Task ParamlessInstanceIsStartedByMessage();
    protected async Task ParamlessInstanceIsStartedByMessage(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store, 
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var syncedFlag = new SyncedFlag();
        var syncedValue = new Synced<string>();

        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async (workflow) =>
            {
                var someMessage = await workflow.Message<SomeMessage>();
                syncedValue.Value = someMessage.Value;
                syncedFlag.Raise();
            }
        );

        await registration.SendMessage(
            flowInstance,
            new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!")
        );
        
        await syncedFlag.WaitForRaised(5_000);
        syncedValue.Value.ShouldBe("SomeValue!");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    #endregion

    public record SomeMessage(string RouteTo, string Value);
}
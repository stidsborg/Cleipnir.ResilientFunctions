using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

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
            inner: async (Workflow workflow) =>
            {
                var someMessage = await workflow.Messages.FirstOfType<SomeMessage>();
                syncedValue.Value = someMessage.Value;
                syncedFlag.Raise();
            },
            new Settings(routes: new RoutingInformation[]
            {
                new RoutingInformation<SomeMessage>(
                    someMsg => Route.To(someMsg.RouteTo)
                )
            })
        );

        await registration.Schedule(flowInstance);

        await registration.PostMessage(new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!"));
        
        await syncedFlag.WaitForRaised();
        syncedValue.Value.ShouldBe("SomeValue!");
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
                var someMessage = await workflow.Messages.FirstOfType<SomeMessage>();
                syncedValue.Value = someMessage.Value;
                syncedFlag.Raise();
            },
            new Settings(routes: new RoutingInformation[]
            {
                new RoutingInformation<SomeMessage>(
                    someMsg => Route.To(someMsg.RouteTo)
                )
            })
        );

        await registration.Schedule(flowInstance.Value, param: "SomeParam");

        await registration.PostMessage(new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!"));
        
        await syncedFlag.WaitForRaised();
        syncedValue.Value.ShouldBe("SomeValue!");
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
                var someMessage = await workflow.Messages.FirstOfType<SomeMessage>();
                syncedValue.Value = someMessage.Value;
                syncedFlag.Raise();
                
                return "SomeResult";
            },
            new Settings(routes: new RoutingInformation[]
            {
                new RoutingInformation<SomeMessage>(
                    someMsg => Route.To(someMsg.RouteTo)
                )
            })
        );

        await registration.Schedule(flowInstance.Value, param: "SomeParam");

        await registration.PostMessage(new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!"));
        
        await syncedFlag.WaitForRaised();
        syncedValue.Value.ShouldBe("SomeValue!");
    }

    #region Route using Correlation

    public abstract Task MessageIsRoutedToParamlessInstanceUsingCorrelationId();
    protected async Task MessageIsRoutedToParamlessInstanceUsingCorrelationId(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store, 
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var correlationId = $"SomeCorrelationId_{Guid.NewGuid().ToString()}";

        var correlationIdRegisteredFlag = new SyncedFlag();
        var syncedFlag = new SyncedFlag();
        var syncedValue = new Synced<string>();
        
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async workflow =>
            {
                await workflow.RegisterCorrelation(correlationId);
                correlationIdRegisteredFlag.Raise();
                
                var someMessage = await workflow.Messages.FirstOfType<SomeCorrelatedMessage>();
                syncedValue.Value = someMessage.Value;
                syncedFlag.Raise();
            },
            new Settings(routes: new RoutingInformation[]
            {
                new RoutingInformation<SomeCorrelatedMessage>(
                    someMsg => Route.Using(someMsg.Correlation)
                )
            })
        );

        await registration.Schedule(flowInstance);
        await correlationIdRegisteredFlag.WaitForRaised();
        
        await registration.PostMessage(new SomeCorrelatedMessage(correlationId, "SomeValue!"));
        
        await syncedFlag.WaitForRaised();
        syncedValue.Value.ShouldBe("SomeValue!");
    }
    
    public abstract Task MessageIsRoutedToMultipleInstancesUsingCorrelationId();
    protected async Task MessageIsRoutedToMultipleInstancesUsingCorrelationId(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance1) = functionId;
        var (_, flowInstance2) = TestFlowId.Create().WithTypeId(flowType);
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store, 
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var correlationId = $"SomeCorrelationId_{Guid.NewGuid().ToString()}";
        
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async workflow =>
            {
                await workflow.RegisterCorrelation(correlationId);
                await workflow.Messages.FirstOfType<SomeCorrelatedMessage>();
            },
            new Settings(routes: new RoutingInformation[]
            {
                new RoutingInformation<SomeCorrelatedMessage>(
                    someMsg => Route.Using(someMsg.Correlation)
                )
            })
        );

        await registration.Schedule(flowInstance1);
        await registration.Schedule(flowInstance2);

        await BusyWait.Until(() => store
            .CorrelationStore
            .GetCorrelations(correlationId)
            .SelectAsync(l => l.Count == 2)
        );
        
        await registration.PostMessage(new SomeCorrelatedMessage(correlationId, "SomeValue!"));

        var controlPanel1 = await registration.ControlPanel(flowInstance1);
        controlPanel1.ShouldNotBeNull();
        await BusyWait.Until(async () =>
        {
            await controlPanel1.Refresh();
            return controlPanel1.Status == Status.Succeeded;
        });
        
        
        var controlPanel2 = await registration.ControlPanel(flowInstance2);
        controlPanel2.ShouldNotBeNull();
        await BusyWait.Until(async () =>
        {
            await controlPanel2.Refresh();
            return controlPanel2.Status == Status.Succeeded;
        });
    }

    #endregion

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
            inner: async (Workflow workflow) =>
            {
                var someMessage = await workflow.Messages.FirstOfType<SomeMessage>();
                syncedValue.Value = someMessage.Value;
                syncedFlag.Raise();
            },
            new Settings(
                messagesDefaultMaxWaitForCompletion: TimeSpan.MaxValue,
                routes: new RoutingInformation[]
                {
                    new RoutingInformation<SomeMessage>(
                        someMsg => Route.To(someMsg.RouteTo)
                    )

                }
            )
        );

        await registration.PostMessage(new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!"));
        
        await syncedFlag.WaitForRaised(5_000);
        syncedValue.Value.ShouldBe("SomeValue!");
    }

    #endregion

    public record SomeMessage(string RouteTo, string Value);
    public record SomeCorrelatedMessage(string Correlation, string Value);
}
using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class RoutingTests
{
    #region Route to FunctionInstance
    
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

        await functionsRegistry.DeliverMessage(new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!"));
        
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

        await functionsRegistry.DeliverMessage(new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!"));
        
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

        await functionsRegistry.DeliverMessage(new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!"));
        
        await syncedFlag.WaitForRaised();
        syncedValue.Value.ShouldBe("SomeValue!");
    }
    
    public abstract Task MessageIsRoutedToSpecificFunctionTypeOnly();
    protected async Task MessageIsRoutedToSpecificFunctionTypeOnly(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var functionId2 = TestFlowId.Create();
        var (flowType2, _) = functionId2;
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store, 
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var syncedFlag = new SyncedFlag();
        var syncedValue = new Synced<string>();
        
        var registration1 = functionsRegistry.RegisterParamless(
            flowType,
            inner: async workflow =>
            {
                var msg = await workflow.Messages.FirstOfType<SomeMessage>();
                syncedValue.Value = msg.Value;
                syncedFlag.Raise();
                
            },
            new Settings(routes: new RoutingInformation[]
            {
                new RoutingInformation<SomeMessage>(
                    someMsg => Route.To(someMsg.RouteTo)
                )
            })
        );
        
        var syncedFlag2 = new SyncedFlag();
        var registration2 = functionsRegistry.RegisterParamless(
            flowType2,
            inner: _ =>
            {
                syncedFlag2.Raise();
                return Task.CompletedTask;
            },
            new Settings(routes: new RoutingInformation[]
            {
                new RoutingInformation<SomeMessage>(
                    someMsg => Route.To(someMsg.RouteTo)
                )
            })
        );

        await functionsRegistry.DeliverMessage(
            registration1.Type.Value,
            new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!"),
            typeof(SomeMessage)
        );
        
        await syncedFlag.WaitForRaised();
        syncedValue.Value.ShouldBe("SomeValue!");

        await Task.Delay(100);
        syncedFlag2.Position.ShouldBe(FlagPosition.Lowered);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    #endregion

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
        
        await functionsRegistry.DeliverMessage(new SomeCorrelatedMessage(correlationId, "SomeValue!"));
        
        await syncedFlag.WaitForRaised();
        syncedValue.Value.ShouldBe("SomeValue!");
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

        await functionsRegistry.DeliverMessage(new SomeMessage(RouteTo: flowInstance.Value, Value: "SomeValue!"));
        
        await syncedFlag.WaitForRaised(5_000);
        syncedValue.Value.ShouldBe("SomeValue!");
    }

    #endregion

    public record SomeMessage(string RouteTo, string Value);
    public record SomeCorrelatedMessage(string Correlation, string Value);
}
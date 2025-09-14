using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class TimeoutTests
{
    public abstract Task ExpiredTimeoutIsAddedToMessages();
    protected async Task ExpiredTimeoutIsAddedToMessages(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(ExpiredTimeoutIsAddedToMessages).ToFlowType();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(250),
                messagesDefaultMaxWaitForCompletion: TimeSpan.MaxValue
            )
        );
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            inner: async Task (string _, Workflow workflow) =>
            {
                var messages = workflow.Messages;
                var timeoutTask = messages.OfType<TimeoutEvent>().First();
                await messages.FlowRegisteredTimeouts.RegisterTimeout("test", expiresIn: TimeSpan.FromMilliseconds(500), publishMessage: true);
                timeoutTask.IsCompleted.ShouldBeFalse();
                var timeout = await timeoutTask;
                timeout.TimeoutId.Id.ShouldBe("test");
            }
        ).Invoke;

        await rAction.Invoke("instanceId", "hello world");
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task ExpiredTimeoutMakesReactiveChainThrowTimeoutException();
    protected async Task ExpiredTimeoutMakesReactiveChainThrowTimeoutException(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(500),
                messagesDefaultMaxWaitForCompletion: TimeSpan.MaxValue
            )
        );
        var rAction = functionsRegistry.RegisterAction(
            flowId.Type,
            inner: async Task (string _, Workflow workflow) =>
            {
                var messages = workflow.Messages;
                await messages
                    .TakeUntilTimeout("TimeoutId#21", expiresIn: TimeSpan.FromMilliseconds(500))
                    .FirstOfType<string>();
            }
        ).Invoke;

        await Should.ThrowAsync<FatalWorkflowException<NoResultException>>(
            () => rAction.Invoke("instanceId", "hello world")
        );
        
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task RegisteredTimeoutIsCancelledAfterReactiveChainCompletes();
    protected async Task  RegisteredTimeoutIsCancelledAfterReactiveChainCompletes(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                messagesDefaultMaxWaitForCompletion: TimeSpan.MaxValue
            )
        );
        var registration = functionsRegistry.RegisterAction(
            flowId.Type,
            inner: Task (string _, Workflow workflow) =>
                workflow
                    .Messages
                    .TakeUntilTimeout("TimeoutId4321", expiresIn: TimeSpan.FromMilliseconds(5_000))
                    .First()
        );

        await registration.Schedule("someInstanceId", "someParam");
        await Task.Delay(10);
        
        var messageWriter = registration.MessageWriters.For(new FlowInstance("someInstanceId"));
        await messageWriter.AppendMessage("someMessage");

        var controlPanel = await registration.ControlPanel("someInstanceId");
        controlPanel.ShouldNotBeNull();

        await controlPanel.WaitForCompletion(allowPostponeAndSuspended: true);
        
        await controlPanel.Refresh();
        
        await controlPanel
            .RegisteredTimeouts
            .All
            .SelectAsync(ts => ts.All(t => t.Status != TimeoutStatus.Registered))
            .ShouldBeTrueAsync();
        
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task PendingTimeoutCanBeRemovedFromControlPanel();
    protected async Task  PendingTimeoutCanBeRemovedFromControlPanel(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                messagesDefaultMaxWaitForCompletion: TimeSpan.Zero
            )
        );
        var registration = functionsRegistry.RegisterParamless(
            flowId.Type,
            inner: Task (workflow) =>
                workflow
                    .Messages
                    .TakeUntilTimeout("TimeoutId4321", expiresIn: TimeSpan.FromMinutes(10))
                    .First()
        );

        await registration.Schedule("someInstanceId");
        
        var controlPanel = await registration.ControlPanel("someInstanceId");
        controlPanel.ShouldNotBeNull();
        await controlPanel.BusyWaitUntil(cp => 
            cp.Status == Status.Postponed, maxWait: TimeSpan.FromSeconds(10));

        var registeredTimeouts = await controlPanel.RegisteredTimeouts.All;
        registeredTimeouts.Count.ShouldBe(1);
        var registeredTimeout = registeredTimeouts.First();

        var id = registeredTimeout.TimeoutId;
        id.Id.ShouldBe("TimeoutId4321");
        id.Type.ShouldBe(EffectType.Timeout);

        await controlPanel.RegisteredTimeouts.Remove(id);
        
        await controlPanel.Refresh();

        await controlPanel.RegisteredTimeouts.All.ShouldBeEmptyAsync();
        
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
     public abstract Task PendingTimeoutCanBeUpdatedFromControlPanel();
    protected async Task  PendingTimeoutCanBeUpdatedFromControlPanel(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                messagesDefaultMaxWaitForCompletion: TimeSpan.Zero
            )
        );
        var registration = functionsRegistry.RegisterParamless(
            flowId.Type,
            inner: Task (workflow) =>
                workflow
                    .Messages
                    .TakeUntilTimeout("TimeoutId4321", expiresIn: TimeSpan.FromMinutes(10))
                    .First()
        );

        await registration.Schedule("someInstanceId");
        
        var controlPanel = await registration.ControlPanel("someInstanceId");
        controlPanel.ShouldNotBeNull();
        await controlPanel.BusyWaitUntil(cp => cp.Status == Status.Postponed);

        var registeredTimeouts = await controlPanel.RegisteredTimeouts.All;
        registeredTimeouts.Count.ShouldBe(1);
        var registeredTimeout = registeredTimeouts.First();

        var id = registeredTimeout.TimeoutId;
        id.Id.ShouldBe("TimeoutId4321");
        id.Type.ShouldBe(EffectType.Timeout);

        await controlPanel.RegisteredTimeouts.Upsert(id, new DateTime(2100, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        
        await controlPanel.Refresh();

        registeredTimeout = (await controlPanel.RegisteredTimeouts.All).Single();
        
        registeredTimeout.TimeoutId.ShouldBe(id);
        
        registeredTimeout.Expiry.ShouldBe(new DateTime(2100, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task ExpiredImplicitTimeoutsAreAddedToMessages();
    protected async Task ExpiredImplicitTimeoutsAreAddedToMessages(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(250),
                messagesDefaultMaxWaitForCompletion: TimeSpan.MaxValue
            )
        );
        
        var expiresAt1 = DateTime.UtcNow.AddMilliseconds(10);
        var expiresAt2 = DateTime.UtcNow.AddMilliseconds(20);
        
        var registration = functionsRegistry.RegisterAction(
            flowType,
            inner: async Task (string _, Workflow workflow) =>
            {
                var messages = workflow.Messages;
                await messages.TakeUntilTimeout(expiresAt1).OfType<string>().FirstOrNone();
                await messages.TakeUntilTimeout(expiresAt2).OfType<string>().FirstOrNone();
            }
        );
        
        await registration.Invoke(flowInstance.Value, "param");
        
        var controlPanel = await registration.ControlPanel(flowInstance);
        controlPanel.ShouldNotBeNull();

        await controlPanel.Restart();
        await controlPanel.Refresh();
        
        (await controlPanel.RegisteredTimeouts.All).Count.ShouldBe(2);

        var messages = await controlPanel.Messages.AsObjects;
        messages.Count.ShouldBe(2);
        messages.OfType<TimeoutEvent>().Count().ShouldBe(2);
        messages.OfType<TimeoutEvent>().Single(t => t.TimeoutId.Id == "0").Expiration.ShouldBe(expiresAt1);
        messages.OfType<TimeoutEvent>().Single(t => t.TimeoutId.Id == "1").Expiration.ShouldBe(expiresAt2);
        
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task TimeoutsWithSameIdsButDifferentContextsDoNotCollide();
    protected async Task TimeoutsWithSameIdsButDifferentContextsDoNotCollide(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(250),
                messagesDefaultMaxWaitForCompletion: TimeSpan.MaxValue
            )
        );
        
        var registration = functionsRegistry.RegisterFunc(
            flowType,
            inner: async Task<Tuple<bool, bool>> (string _, Workflow workflow) =>
            {
                var (effect, messages, _) = workflow;
                var didFirstTimeout = await effect.Capture("First", () => 
                    messages.TakeUntilTimeout("TimeoutId", TimeSpan.FromMilliseconds(10))
                        .FirstOrNone()
                        .SelectAsync(o => !o.HasValue)
                    );

                await messages.AppendMessage("SomeMessage");
                
                var didSecondTimeout = await effect.Capture("Second", () => 
                    messages.TakeUntilTimeout("TimeoutId", TimeSpan.FromSeconds(30))
                        .FirstOrNone()
                        .SelectAsync(o => !o.HasValue)
                    );

                return Tuple.Create(didFirstTimeout, didSecondTimeout);
            }
        );
        
        var (firstTimeout, secondTimeout) = await registration.Invoke(flowInstance.Value, "param");
        firstTimeout.ShouldBeTrue();
        secondTimeout.ShouldBeFalse();
        
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task ProvidedUtcNowDelegateIsUsedInWatchdog();
    protected async Task ProvidedUtcNowDelegateIsUsedInWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        
        DateTime now = DateTime.UtcNow;
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(250),
                utcNow: () => now
            )
        );
        
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async Task (workflow) =>
            {
                await workflow.Delay(now.AddMilliseconds(100));
            }
        );
        await registration.Schedule("SomeInstance");

        var cp = await registration.ControlPanel("SomeInstance").ShouldNotBeNullAsync();
        await cp.BusyWaitUntil(c => c.Status == Status.Postponed);

        await Task.Delay(250);
        
        await cp.ScheduleRestart();
        await cp.BusyWaitUntil(c => c.Status == Status.Postponed);

        now = now.AddHours(1);

        await cp.Refresh();
        await cp.BusyWaitUntil(c => c.Status == Status.Succeeded);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
}
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

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
                await messages.RegisteredTimeouts.RegisterTimeout("test", expiresIn: TimeSpan.FromMilliseconds(500));
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

        await Should.ThrowAsync<NoResultException>(
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

        var messageWriter = registration.MessageWriters.For(new FlowInstance("someInstanceId"));
        await messageWriter.AppendMessage("someMessage");

        var controlPanel = await registration.ControlPanel("someInstanceId");
        controlPanel.ShouldNotBeNull();

        await controlPanel.WaitForCompletion(allowPostponeAndSuspended: true);
        
        await controlPanel.Refresh();
        var registeredTimeouts = await controlPanel.RegisteredTimeouts.All;
        Console.WriteLine(registeredTimeouts);
        await controlPanel
            .RegisteredTimeouts
            .All
            .SelectAsync(ts => ts.Count == 0)
            .ShouldBeTrueAsync();
        
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
        
        (await controlPanel.RegisteredTimeouts.All).Count.ShouldBe(0);

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
}
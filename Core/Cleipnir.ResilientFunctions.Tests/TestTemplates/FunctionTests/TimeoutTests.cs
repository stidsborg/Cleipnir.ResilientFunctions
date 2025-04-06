using System;
using System.Linq;
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
                
                var timeoutTask = messages.OfType<TimeoutEvent>().First(maxWait: TimeSpan.FromSeconds(10));
                timeoutTask.IsCompleted.ShouldBeFalse();
                await messages.TakeUntilTimeout("TimeoutId", TimeSpan.FromMilliseconds(500)).Completion();
                var timeout = await timeoutTask;
                timeout.TimeoutId.Id.ShouldBe("TimeoutId");
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
                    .FirstOfType<string>(maxWait: TimeSpan.FromSeconds(5));
            }
        ).Invoke;

        await Should.ThrowAsync<FatalWorkflowException<NoResultException>>(
            () => rAction.Invoke("instanceId", "hello world")
        );
        
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
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(1_000),
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
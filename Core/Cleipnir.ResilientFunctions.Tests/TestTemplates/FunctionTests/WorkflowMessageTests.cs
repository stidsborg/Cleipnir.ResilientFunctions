using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class WorkflowMessageTests
{
    // Tests for: Task<T> Message<T>()
    public abstract Task WorkflowMessagePullsMessageSuccessfully();
    public async Task WorkflowMessagePullsMessageSuccessfully(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessagePullsMessageSuccessfully),
            async Task<string> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<string>();
                return message;
            }
        );

        await registration.Schedule("instanceId", "");
        await registration.SendMessage("instanceId", "hello world");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await controlPanel.BusyWaitUntil(c => c.Status == Status.Succeeded, maxWait: TimeSpan.FromSeconds(10));
        controlPanel.Result.ShouldBe("hello world");

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task WorkflowMessagePullsFirstMessageWhenMultipleMessagesExist();
    public async Task WorkflowMessagePullsFirstMessageWhenMultipleMessagesExist(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessagePullsFirstMessageWhenMultipleMessagesExist),
            async Task<string> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<string>();
                return message;
            }
        );

        await registration.SendMessage("instanceId", "first");
        await registration.SendMessage("instanceId", "second");

        await registration.Schedule("instanceId", "");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await controlPanel.BusyWaitUntil(c => c.Status == Status.Succeeded, maxWait: TimeSpan.FromSeconds(10));
        controlPanel.Result.ShouldBe("first");

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    // Tests for: Task<T?> Message<T>(DateTime waitUntil)
    public abstract Task WorkflowMessageWithDateTimeReturnsMessageBeforeTimeout();
    public async Task WorkflowMessageWithDateTimeReturnsMessageBeforeTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessageWithDateTimeReturnsMessageBeforeTimeout),
            async Task<string?> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<string>(DateTime.UtcNow.AddSeconds(10));
                return message;
            }
        );

        await registration.Schedule("instanceId", "");
        await registration.SendMessage("instanceId", "hello world");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await controlPanel.BusyWaitUntil(c => c.Status == Status.Succeeded, maxWait: TimeSpan.FromSeconds(10));
        controlPanel.Result.ShouldBe("hello world");

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task WorkflowMessageWithDateTimeReturnsNullOnTimeout();
    public async Task WorkflowMessageWithDateTimeReturnsNullOnTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessageWithDateTimeReturnsNullOnTimeout),
            async Task<string?> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<string>(DateTime.UtcNow.AddMilliseconds(100));
                return message;
            }
        );

        await registration.Schedule("instanceId", "");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        }, maxWait: TimeSpan.FromSeconds(10));

        controlPanel.Result.ShouldBeNull();

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    // Tests for: Task<T?> Message<T>(TimeSpan waitFor)
    public abstract Task WorkflowMessageWithTimeSpanReturnsMessageBeforeTimeout();
    public async Task WorkflowMessageWithTimeSpanReturnsMessageBeforeTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessageWithTimeSpanReturnsMessageBeforeTimeout),
            async Task<string?> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<string>(TimeSpan.FromSeconds(10));
                return message;
            }
        );

        await registration.Schedule("instanceId", "");
        await registration.SendMessage("instanceId", "hello world");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await controlPanel.BusyWaitUntil(c => c.Status == Status.Succeeded, maxWait: TimeSpan.FromSeconds(10));
        controlPanel.Result.ShouldBe("hello world");

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task WorkflowMessageWithTimeSpanReturnsNullOnTimeout();
    public async Task WorkflowMessageWithTimeSpanReturnsNullOnTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessageWithTimeSpanReturnsNullOnTimeout),
            async Task<string?> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<string>(TimeSpan.FromMilliseconds(100));
                return message;
            }
        );

        await registration.Schedule("instanceId", "");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        }, maxWait: TimeSpan.FromSeconds(10));

        controlPanel.Result.ShouldBeNull();

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    // Tests for: Task<T> Message<T>(Func<T, bool> filter)
    public abstract Task WorkflowMessageWithFilterReturnsMatchingMessage();
    public async Task WorkflowMessageWithFilterReturnsMatchingMessage(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessageWithFilterReturnsMatchingMessage),
            async Task<string> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<string>(s => s.Length > 5);
                return message;
            }
        );

        await registration.Schedule("instanceId", "");
        await registration.SendMessage("instanceId", "short");
        await registration.SendMessage("instanceId", "longer string");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await controlPanel.BusyWaitUntil(c => c.Status == Status.Succeeded, maxWait: TimeSpan.FromSeconds(10));
        controlPanel.Result.ShouldBe("longer string");

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task WorkflowMessageWithFilterIgnoresNonMatchingMessages();
    public async Task WorkflowMessageWithFilterIgnoresNonMatchingMessages(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessageWithFilterIgnoresNonMatchingMessages),
            async Task<string> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<string>(s => s.StartsWith("hello"));
                return message;
            }
        );

        await registration.Schedule("instanceId", "");
        await registration.SendMessage("instanceId", "goodbye");
        await registration.SendMessage("instanceId", "hello world");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await controlPanel.BusyWaitUntil(c => c.Status == Status.Succeeded, maxWait: TimeSpan.FromSeconds(10));
        controlPanel.Result.ShouldBe("hello world");

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    // Tests for: Task<T?> Message<T>(Func<T, bool> filter, DateTime waitUntil)
    public abstract Task WorkflowMessageWithFilterAndDateTimeReturnsMatchingMessageBeforeTimeout();
    public async Task WorkflowMessageWithFilterAndDateTimeReturnsMatchingMessageBeforeTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessageWithFilterAndDateTimeReturnsMatchingMessageBeforeTimeout),
            async Task<string?> (string _, Workflow workflow) =>
            {
                string? message = await workflow.Message<string>(s => s.Length > 5, DateTime.UtcNow.AddSeconds(10));
                return message;
            }
        );

        await registration.Schedule("instanceId", "");
        await registration.SendMessage("instanceId", "short");
        await registration.SendMessage("instanceId", "longer string");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await controlPanel.BusyWaitUntil(c => c.Status == Status.Succeeded, maxWait: TimeSpan.FromSeconds(10));
        controlPanel.Result.ShouldBe("longer string");

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task WorkflowMessageWithFilterAndDateTimeReturnsNullOnTimeout();
    public async Task WorkflowMessageWithFilterAndDateTimeReturnsNullOnTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessageWithFilterAndDateTimeReturnsNullOnTimeout),
            async Task<string?> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<string>(s => s.Length > 5, DateTime.UtcNow.AddMilliseconds(100));
                return message;
            }
        );

        await registration.Schedule("instanceId", "");
        await registration.SendMessage("instanceId", "short");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        }, maxWait: TimeSpan.FromSeconds(10));

        controlPanel.Result.ShouldBeNull();

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    // Tests for: Task<T?> Message<T>(Func<T, bool> filter, TimeSpan waitFor)
    public abstract Task WorkflowMessageWithFilterAndTimeSpanReturnsMatchingMessageBeforeTimeout();
    public async Task WorkflowMessageWithFilterAndTimeSpanReturnsMatchingMessageBeforeTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessageWithFilterAndTimeSpanReturnsMatchingMessageBeforeTimeout),
            async Task<string?> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<string>(s => s.Length > 5, TimeSpan.FromSeconds(10));
                return message;
            }
        );

        await registration.Schedule("instanceId", "");
        await registration.SendMessage("instanceId", "short");
        await registration.SendMessage("instanceId", "longer string");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await controlPanel.BusyWaitUntil(c => c.Status == Status.Succeeded, maxWait: TimeSpan.FromSeconds(10));
        controlPanel.Result.ShouldBe("longer string");

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task WorkflowMessageWithFilterAndTimeSpanReturnsNullOnTimeout();
    public async Task WorkflowMessageWithFilterAndTimeSpanReturnsNullOnTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessageWithFilterAndTimeSpanReturnsNullOnTimeout),
            async Task<string?> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<string>(s => s.Length > 5, TimeSpan.FromMilliseconds(100));
                return message;
            }
        );

        await registration.Schedule("instanceId", "");
        await registration.SendMessage("instanceId", "short");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        }, maxWait: TimeSpan.FromSeconds(10));

        controlPanel.Result.ShouldBeNull();

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    // Test for idempotency with effect IDs
    public abstract Task WorkflowMessageIsIdempotentAcrossRestarts();
    public async Task WorkflowMessageIsIdempotentAcrossRestarts(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var invocationCount = 0;
        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessageIsIdempotentAcrossRestarts),
            async Task<string> (string _, Workflow workflow) =>
            {
                invocationCount++;
                var message = await workflow.Message<string>();
                if (invocationCount == 1)
                    throw new Exception("Simulated failure");
                return message;
            }
        );

        await registration.SendMessage("instanceId", "hello world");

        await registration.Schedule("instanceId", "");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Failed;
        }, maxWait: TimeSpan.FromSeconds(10));

        await controlPanel.Restart();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        }, maxWait: TimeSpan.FromSeconds(10));

        controlPanel.Result.ShouldBe("hello world");
        invocationCount.ShouldBe(2);

        // The first invocation throws an exception which goes to the unhandled exception handler
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(1);
        unhandledExceptionHandler.ThrownExceptions[0].InnerException?.Message.ShouldBe("Simulated failure");
    }

    // Test for multiple sequential Message calls
    public abstract Task MultipleSequentialMessageCallsReturnDifferentMessages();
    public async Task MultipleSequentialMessageCallsReturnDifferentMessages(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(MultipleSequentialMessageCallsReturnDifferentMessages),
            async Task<string> (string _, Workflow workflow) =>
            {
                var first = await workflow.Message<string>();
                var second = await workflow.Message<string>();
                return first + " " + second;
            }
        );

        await registration.Schedule("instanceId", "");
        await registration.SendMessage("instanceId", "hello");
        await registration.SendMessage("instanceId", "world");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await controlPanel.BusyWaitUntil(c => c.Status == Status.Succeeded, maxWait: TimeSpan.FromSeconds(10));
        controlPanel.Result.ShouldBe("hello world");

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    // Test for different message types
    public abstract Task WorkflowMessageCanPullDifferentMessageTypes();
    public async Task WorkflowMessageCanPullDifferentMessageTypes(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(WorkflowMessageCanPullDifferentMessageTypes),
            async Task<string> (string _, Workflow workflow) =>
            {
                var stringMessage = await workflow.Message<string>();
                var secondMessage = await workflow.Message<string>();
                return $"{stringMessage}:{secondMessage}";
            }
        );

        await registration.Schedule("instanceId", "");
        await registration.SendMessage("instanceId", "test");
        await registration.SendMessage("instanceId", "value");

        var controlPanel = await registration.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await controlPanel.BusyWaitUntil(c => c.Status == Status.Succeeded, maxWait: TimeSpan.FromSeconds(10));
        controlPanel.Result.ShouldBe("test:value");

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
}

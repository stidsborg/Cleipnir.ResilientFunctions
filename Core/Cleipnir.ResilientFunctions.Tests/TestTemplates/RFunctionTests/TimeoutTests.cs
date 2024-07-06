using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
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
        var functionTypeId = nameof(ExpiredTimeoutIsAddedToMessages).ToFunctionTypeId();
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
        var rFunc = functionsRegistry.RegisterAction(
            functionTypeId,
            inner: async Task (string _, Workflow workflow) =>
            {
                var messages = workflow.Messages;
                var timeoutTask = messages.OfType<TimeoutEvent>().First();
                await messages.TimeoutProvider.RegisterTimeout("test", expiresIn: TimeSpan.FromMilliseconds(500));
                timeoutTask.IsCompleted.ShouldBeFalse();
                var timeout = await timeoutTask;
                timeout.TimeoutId.ShouldBe("test");
            }
        ).Invoke;

        await rFunc.Invoke("instanceId", "hello world");
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
}
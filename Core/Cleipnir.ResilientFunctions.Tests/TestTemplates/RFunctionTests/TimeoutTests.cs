using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class TimeoutTests
{
    public abstract Task ExpiredTimeoutIsAddedToEventSource();

    protected async Task ExpiredTimeoutIsAddedToEventSource(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ExpiredTimeoutIsAddedToEventSource).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                signOfLifeFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero,
                timeoutCheckFrequency: TimeSpan.FromMilliseconds(1)
            )
        );
        var rFunc = rFunctions.RegisterAction(
            functionTypeId,
            inner: async Task (string _, Context context) =>
            {
                var es = await context.EventSource;
                var timeoutTask = es.OfType<TimeoutEvent>().Next();
                await es.TimeoutProvider.RegisterTimeout("test", expiresIn: TimeSpan.FromMilliseconds(500));
                timeoutTask.IsCompleted.ShouldBeFalse();
                var timeout = await timeoutTask;
                timeout.TimeoutId.ShouldBe("test");
            }
        ).Invoke;

        await rFunc.Invoke("instanceId", "hello world");
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
}
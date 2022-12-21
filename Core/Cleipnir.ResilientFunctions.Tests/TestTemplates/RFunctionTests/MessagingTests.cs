﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class MessagingTests
{
    public abstract Task FunctionCompletesAfterAwaitedMessageIsReceived();
    public async Task FunctionCompletesAfterAwaitedMessageIsReceived(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionHandler.Catch));

        var rAction = rFunctions.RegisterFunc(
            nameof(FunctionCompletesAfterAwaitedMessageIsReceived),
            inner: async Task<string> (string _, Context context) =>
            {
                var es = await context.EventSource;
                return await es.OfType<string>().Next();
            }
        );

        var invocationTask = rAction.Invoke("instanceId", "");
        await Task.Delay(100);
        invocationTask.IsCompleted.ShouldBeFalse();
        
        var eventSourceWriter = rAction.EventSourceWriters.For("instanceId");
        await eventSourceWriter.Append("hello world");
        var result = await invocationTask;
        result.ShouldBe("hello world");
    }
    
    public abstract Task FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist();
    public async Task FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionHandler.Catch));

        var rAction = rFunctions.RegisterFunc(
            nameof(FunctionCompletesAfterAwaitedMessageIsReceived),
            inner: async Task<string> (string _, Context context) =>
            {
                var es = await context.EventSource;
                return await es.OfType<string>().NextOrSuspend();
            }
        );

        await Should.ThrowAsync<Exception>(() => rAction.Invoke("instanceId", ""));
    }
}
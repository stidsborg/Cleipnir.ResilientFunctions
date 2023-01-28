﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
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
        await eventSourceWriter.AppendEvent("hello world");
        var result = await invocationTask;
        result.ShouldBe("hello world");
        
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist();
    public async Task FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var functionId = new FunctionId(nameof(FunctionCompletesAfterAwaitedMessageIsReceived),"instanceId");
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionHandler.Catch));

        var rAction = rFunctions.RegisterFunc(
            functionId.TypeId,
            inner: async Task<string> (string _, Context context) =>
            {
                var es = await context.EventSource;
                return await es.OfType<string>().NextOrSuspend();
            }
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(() =>
            rAction.Invoke(functionId.InstanceId.Value, "")
        );
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task TimeoutEventCausesSuspendedFunctionToBeReInvoked();
    public async Task TimeoutEventCausesSuspendedFunctionToBeReInvoked(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var functionId = new FunctionId(nameof(TimeoutEventCausesSuspendedFunctionToBeReInvoked),"instanceId");
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionHandler.Catch));

        var rFunc = rFunctions.RegisterFunc(
            functionId.TypeId,
            inner: async Task<Tuple<bool, string>> (string _, Context context) =>
            {
                var es = await context.EventSource;

                try
                {
                    await es
                        .OfType<string>()
                        .SuspendUntilNextOrTimeoutEventFired("timeoutId1", expiresIn: TimeSpan.FromMilliseconds(250));
                }
                catch (TimeoutException)
                {
                    var success = es.OfType<Timeout>().TryNext(out var timeoutId);
                    return Tuple.Create(success, timeoutId?.TimeoutId ?? "");
                }

                throw new Exception("The code should never reach this point");
            }
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(() =>
            rFunc.Invoke(functionId.InstanceId.Value, "")
        );

        var controlPanel = await rFunc.ControlPanel.For("instanceId");
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });


        controlPanel.Result.ShouldNotBeNull();
        var (success, timeoutId) = controlPanel.Result;
        success.ShouldBeTrue();
        timeoutId.ShouldBe("timeoutId1");
        
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
}
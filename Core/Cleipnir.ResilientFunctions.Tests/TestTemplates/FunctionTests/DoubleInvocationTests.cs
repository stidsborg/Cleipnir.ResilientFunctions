﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class DoubleInvocationTests
{
    public abstract Task SecondInvocationWaitsForAndReturnsSuccessfulResult();
    protected async Task SecondInvocationWaitsForAndReturnsSuccessfulResult(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var syncTask = new TaskCompletionSource();
        var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            (string input) => syncTask.Task.ContinueWith(_ => input)
        );
        
        var invocationTask = rFunc.Invoke(flowInstance.Value, param: "Hallo World");
        invocationTask.IsCompleted.ShouldBeFalse();
        
        var secondInvocationTask = rFunc.Invoke(flowInstance.Value, param: "Hallo World");
        secondInvocationTask.IsCompleted.ShouldBeFalse();
        
        syncTask.SetResult();

        await BusyWait.Until(() => secondInvocationTask.IsCompletedSuccessfully);
        
        secondInvocationTask.Result.ShouldBe("Hallo World");
        
        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
    
    public abstract Task SecondInvocationFailsOnSuspendedFlow();
    protected async Task SecondInvocationFailsOnSuspendedFlow(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            (string input) => Suspend.Invocation.ToResult<string>().ToTask()
        );
        
        await Safe.Try(() => rFunc.Invoke(flowInstance.Value, param: "Hallo World"));
        
        var secondInvocationTask = rFunc.Invoke(flowInstance.Value, param: "Hallo World");
        await BusyWait.Until(() => secondInvocationTask.IsCompleted);

        await Should.ThrowAsync<InvocationSuspendedException>(secondInvocationTask);
        
        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
    
    public abstract Task SecondInvocationFailsOnPostponedFlow();
    protected async Task SecondInvocationFailsOnPostponedFlow(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            (string input) => Postpone.Until(DateTime.UtcNow.AddMilliseconds(100_000)).ToResult<string>().ToTask()
        );
        
        await Safe.Try(() => rFunc.Invoke(flowInstance.Value, param: "Hallo World"));
        
        var secondInvocationTask = rFunc.Invoke(flowInstance.Value, param: "Hallo World");
        await BusyWait.Until(() => secondInvocationTask.IsCompleted);

        await Should.ThrowAsync<InvocationPostponedException>(secondInvocationTask);
        
        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
    
    public abstract Task SecondInvocationFailsOnFailedFlow();
    protected async Task SecondInvocationFailsOnFailedFlow(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            (string input) => Fail.WithException(new InvalidOperationException("Oh no")).ToResult<string>().ToTask()
        );

        await Safe.Try(() => rFunc.Invoke(flowInstance.Value, param: "Hallo World"));
        
        var secondInvocationTask = rFunc.Invoke(flowInstance.Value, param: "Hallo World");
        await BusyWait.Until(() => secondInvocationTask.IsCompleted);

        try
        {
            await secondInvocationTask;
            Assert.Fail("Expected task to fail");
        }
        catch (FatalWorkflowException e)
        {
            Assert.IsTrue(e.ErrorType == typeof(InvalidOperationException));
        }
        
        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
}
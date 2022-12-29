using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class SuspensionTests
{
    public abstract Task ActionCanBeSuspended();
    protected async Task ActionCanBeSuspended(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ActionCanBeSuspended).ToFunctionTypeId();
        var functionInstanceId = "functionInstanceId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            Result(string _) => Suspend.Until(1)
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rAction.Invoke(functionInstanceId, "hello world")
        );

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        sf.SuspendedUntilEventSourceCount.ShouldBe(1);

        var epoch = await store.IsFunctionSuspendedAndEligibleForReInvocation(functionId);
        epoch.ShouldBeNull();
    }
    
    public abstract Task FunctionCanBeSuspended();
    protected async Task FunctionCanBeSuspended(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(FunctionCanBeSuspended).ToFunctionTypeId();
        var functionInstanceId = "functionInstanceId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            Result<string>(string _) => Suspend.Until(1)
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rFunc.Invoke(functionInstanceId, "hello world")
        );

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        sf.SuspendedUntilEventSourceCount.ShouldBe(1);
        
        var epoch = await store.IsFunctionSuspendedAndEligibleForReInvocation(functionId);
        epoch.ShouldBeNull();
    }
    
    public abstract Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded();
    protected async Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded).ToFunctionTypeId();
        var functionInstanceId = "functionInstanceId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            Result<string>(string _) => Suspend.Until(2)
        );

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rFunc.Invoke(functionInstanceId, "hello world")
        );

        (await store.GetEligibleSuspendedFunctions(functionTypeId)).ShouldBeEmpty();

        await rFunc.EventSourceWriters.For(functionInstanceId).Append("hello universe");

        (await store.GetEligibleSuspendedFunctions(functionTypeId)).ShouldBeEmpty();
        
        await rFunc.EventSourceWriters.For(functionInstanceId).Append("hello multiverse");
        
        var eligibleFunctions = await store
            .GetEligibleSuspendedFunctions(functionTypeId)
            .ToListAsync();
        
        eligibleFunctions.Count.ShouldBe(1);
        eligibleFunctions[0].InstanceId.ShouldBe(functionInstanceId);
        eligibleFunctions[0].Epoch.ShouldBe(0);
        
        var epoch = await store.IsFunctionSuspendedAndEligibleForReInvocation(functionId);
        epoch.ShouldNotBeNull();
        epoch.Value.ShouldBe(0);
    }
    
    public abstract Task EligibleSuspendedFunctionIsPickedUpByWatchdog();
    protected async Task EligibleSuspendedFunctionIsPickedUpByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(EligibleSuspendedFunctionIsPickedUpByWatchdog).ToFunctionTypeId();
        var functionInstanceId = "functionInstanceId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch, suspensionCheckFrequency: TimeSpan.FromMilliseconds(100))
        );

        var flag = new SyncedFlag();
        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionTypeId,
            Result<string>(_) =>
            {
                if (flag.IsRaised) return "success";
                flag.Raise();
                return Suspend.Until(1);
            });

        await Should.ThrowAsync<FunctionInvocationSuspendedException>(
            () => rFunc.Invoke(functionInstanceId, "hello world")
        );

        await rFunc.EventSourceWriters.For(functionInstanceId).Append("hello universe");
        
        await BusyWait.Until(
            () => store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
    }
}
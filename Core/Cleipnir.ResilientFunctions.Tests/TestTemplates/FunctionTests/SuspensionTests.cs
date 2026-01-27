using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;
using FlagPosition = Cleipnir.ResilientFunctions.Tests.Utils.FlagPosition;
using SyncedFlag = Cleipnir.ResilientFunctions.Tests.Utils.SyncedFlag;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class SuspensionTests
{
    public abstract Task ActionCanBeSuspended();
    protected async Task ActionCanBeSuspended(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch, watchdogCheckFrequency: TimeSpan.FromSeconds(60))
        );

        var rAction = functionsRegistry.RegisterAction(
            flowType,
            Task<Result<Unit>> (string _) => throw new SuspendInvocationException()
        );

        await Should.ThrowAsync<InvocationSuspendedException>(
            () => rAction.Invoke(flowInstance.Value, "hello world")
        );

        await BusyWait.Until(() =>
            store.GetFunction(rAction.MapToStoredId(functionId.Instance)).SelectAsync(sf => sf?.Status == Status.Suspended)
        );
        
        var sf = await store.GetFunction(rAction.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task FunctionCanBeSuspended();
    protected async Task FunctionCanBeSuspended(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (typeId, instanceId) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch, watchdogCheckFrequency: TimeSpan.FromSeconds(60))
        );
        
        var rFunc = functionsRegistry.RegisterFunc(
            typeId,
            Task<Result<string>>(string _) => Suspend.Invocation.ToResult<string>().ToTask()
        );

        await Should.ThrowAsync<InvocationSuspendedException>(
            () => rFunc.Invoke(instanceId.Value, "hello world")
        );
        
        await BusyWait.Until(() =>
            store.GetFunction(rFunc.MapToStoredId(functionId.Instance)).SelectAsync(sf => sf?.Status == Status.Suspended)
        );

        var sf = await store.GetFunction(rFunc.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded();
    protected async Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var invocations = 0;
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            Task<Result<string>> (string _) =>
            {
                if (invocations == 0)
                {
                    invocations++;
                    return Suspend.Invocation.ToResult<string>().ToTask();
                }

                invocations++;
                return Succeed.WithValue("completed").ToTask();
            });

        await Should.ThrowAsync<InvocationSuspendedException>(
            () => rFunc.Invoke(flowInstance.Value, "hello world")
        );

        await Task.Delay(250);

        var messagesWriter = rFunc.MessageWriters.For(flowInstance); 
        await messagesWriter.AppendMessage("hello multiverse");

        await BusyWait.Until(() => invocations == 2);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task PostponedFunctionIsResumedAfterEventIsAppendedToMessages();
    protected async Task PostponedFunctionIsResumedAfterEventIsAppendedToMessages(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var invocations = 0;
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            Task<Result<string>> (string _) =>
            {
                if (invocations == 0)
                {
                    invocations++;
                    return Postpone.Until(DateTime.UtcNow.AddHours(1)).ToResult<string>().ToTask();
                }

                invocations++;
                return Succeed.WithValue("completed").ToTask();
            });

        await Should.ThrowAsync<InvocationPostponedException>(
            () => rFunc.Invoke(flowInstance.Value, "hello world")
        );

        await Task.Delay(250);

        var messagesWriter = rFunc.MessageWriters.For(flowInstance); 
        await messagesWriter.AppendMessage("hello multiverse");

        await BusyWait.Until(() => invocations == 2);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task EligibleSuspendedFunctionIsPickedUpByWatchdog();
    protected async Task EligibleSuspendedFunctionIsPickedUpByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(EligibleSuspendedFunctionIsPickedUpByWatchdog).ToFlowType();
        var flowInstance = "flowInstance";
        var functionId = new FlowId(flowType, flowInstance);
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var flag = new SyncedFlag();
        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            flowType,
            Task<Result<string>> (_) =>
            {
                if (flag.IsRaised) return Succeed.WithValue("success").ToTask();
                flag.Raise();
                return Suspend.Invocation.ToResult<string>().ToTask();
            });

        await Should.ThrowAsync<InvocationSuspendedException>(
            () => rFunc.Invoke(flowInstance, "hello world")
        );

        await rFunc.MessageWriters.For(flowInstance.ToFlowInstance()).AppendMessage("hello universe");
        
        await BusyWait.Until(
            () => store.GetFunction(rFunc.MapToStoredId(functionId.Instance)).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag();
    protected async Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowInstance = "flowInstance";

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            nameof(SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag),
            async Task<string> (string param, Workflow workflow) =>
            {
                var next = await workflow.Message<string>();
                return next;
            }
        );

        await Should.ThrowAsync<InvocationSuspendedException>(() =>
            registration.Invoke(flowInstance, "hello world")
        );

        var messagesWriter = registration.MessageWriters.For(flowInstance.ToFlowInstance());
        await messagesWriter.AppendMessage("hello universe");

        var controlPanel = await registration.ControlPanel(flowInstance);
        controlPanel.ShouldNotBeNull();
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });

        await controlPanel.Refresh();
        controlPanel.Result.ShouldBe("hello universe");
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleByWatchdog();
    protected async Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var registration = functionsRegistry.RegisterFunc(
            flowType,
            async Task<string> (string param, Workflow workflow) =>
            {
                var next = await workflow.Message<string>();
                return next;
            }
        );

        await Should.ThrowAsync<InvocationSuspendedException>(() =>
            registration.Invoke(flowInstance.Value, "hello world")
        );

        var messagesWriter = registration.MessageWriters.For(flowInstance);
        await messagesWriter.AppendMessage("hello universe");

        var controlPanel = await registration.ControlPanel(flowInstance);
        controlPanel.ShouldNotBeNull();
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });

        await controlPanel.Refresh();
        controlPanel.Result.ShouldBe("hello universe");
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ParamlessFunctionWithPrefilledMessageCompletes();
    protected async Task ParamlessFunctionWithPrefilledMessageCompletes(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var syncedValue = new Synced<string>();
        
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async Task (workflow) =>
            {
                var msg = await workflow.Message<string>();
                syncedValue.Value = msg;
            }
        );

        await registration.SendMessage(
            flowInstance.Value.ToFlowInstance(),
            message: "Hello!"
        );

        var controlPanel = await registration.ControlPanel(flowInstance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.WaitForCompletion(allowPostponeAndSuspended: true);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task StartedParentCanWaitForChildActionCompletion();
    protected async Task StartedParentCanWaitForChildActionCompletion(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parentFunctionId = new FlowId($"ParentFunction{Guid.NewGuid()}", Guid.NewGuid().ToString());

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var child = functionsRegistry.RegisterFunc(
            flowType: $"ChildFunction{Guid.NewGuid()}",
            inner: Task<string> (string param) => param.ToUpper().ToTask()
        );

        var parent = functionsRegistry.RegisterFunc(
            parentFunctionId.Type,
            async Task<string> (string param, Workflow workflow) =>
            {
                var words = param.Split(" ");

                var scheduled = await child.BulkSchedule(
                    words.Select(word => new BulkWork<string>($"Child#{word}", word))
                );

                var results = await scheduled.Completion();
                return results.StringJoin(" ");
            });

        var param = "hello world and universe";
        try
        {
            var result = await parent.Schedule(parentFunctionId.Instance.Value, param).Completion();
            result.ShouldBe(param.ToUpper());
        }
        catch (Exception ex)
        {
            var parentStatus = await store
                .GetFunction(parentFunctionId.ToStoredId(parent.StoredType))
                .SelectAsync(sf => new { Name = "Parent", Status = sf?.Status });
            var childrenStatus = await param
                .Split(" ")
                .Select((_, i) => store
                    .GetFunction(StoredId.Create(child.StoredType, $"Child#{i}"))
                    .SelectAsync(sf => new { Name = $"Child#{i}", Status = sf?.Status })
                )
                .AwaitAll();
            
            var statusStr = parentStatus.AsEnumerable()
                .Concat(childrenStatus)
                .Select(a => $"{a.Name}: {a.Status}")
                .JoinStrings(Environment.NewLine);
            
            throw new TimeoutException(
                "FunctionStates: " + Environment.NewLine + statusStr,
                innerException: ex
            );
        }
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ChildCanReturnResultToParent();
    protected async Task ChildCanReturnResultToParent(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parentFunctionId = new FlowId($"ParentFunction{Guid.NewGuid()}", Guid.NewGuid().ToString());

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        
        var child = functionsRegistry.RegisterFunc(
            flowType: $"ChildFunction{Guid.NewGuid()}",
            inner: (string param) => param.ToUpper().ToTask()
        );

        var parent = functionsRegistry.RegisterFunc(
            parentFunctionId.Type,
            inner: Task<string> (string param) => child.Schedule("SomeChildInstance#1", param).Completion()
        );

        var parentResult = await parent.Schedule(parentFunctionId.Instance.Value, param: "hello").Completion();
        parentResult.ShouldBe("HELLO");
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ParentCanWaitForChildAction();
    protected async Task ParentCanWaitForChildAction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parentFunctionId = new FlowId($"ParentFunction{Guid.NewGuid()}", Guid.NewGuid().ToString());

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        
        var child = functionsRegistry.RegisterAction(
            flowType: $"ChildFunction{Guid.NewGuid()}",
            inner: (string _) => Task.Delay(100)
        );

        var parent = functionsRegistry.RegisterAction(
            parentFunctionId.Type,
            inner: Task (string param) => child.Schedule("SomeChildInstance#1", param).Completion()
        );

        await parent.Schedule(parentFunctionId.Instance.Value, param: "hello").Completion(maxWait: TimeSpan.FromSeconds(100));
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ParentCanWaitForFailedChildAction();
    protected async Task ParentCanWaitForFailedChildAction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parentFunctionId = new FlowId($"ParentFunction{Guid.NewGuid()}", Guid.NewGuid().ToString());

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        
        var child = functionsRegistry.RegisterAction(
            flowType: $"ChildFunction{Guid.NewGuid()}",
            inner: (string _) => throw new InvalidOperationException("oh no")
        );

        var parent = functionsRegistry.RegisterAction(
            parentFunctionId.Type,
            inner: Task (string param) => child.Schedule("SomeChildInstance#1", param).Completion()
        );
        
        await Should.ThrowAsync<FatalWorkflowException>(
            () => parent.Schedule(parentFunctionId.Instance.Value, param: "hello").Completion(maxWait: TimeSpan.FromSeconds(100))
        );
        
        unhandledExceptionHandler.ThrownExceptions.ShouldNotBeEmpty();
    }
    
    public abstract Task PublishFromChildActionStressTest();
    protected async Task PublishFromChildActionStressTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parentFunctionId = new FlowId($"ParentFunction{Guid.NewGuid()}", Guid.NewGuid().ToString());
        const int numberOfChildren = 100;
        
        using var functionsRegistry = new FunctionsRegistry(store);

        FuncRegistration<string, List<string>>? parent = null;
        var child = functionsRegistry.RegisterAction(
            $"ChildFunction{Guid.NewGuid()}",
            inner: (string param) =>
                parent!.MessageWriters
                    .For(parentFunctionId.Instance)
                    .AppendMessage(param, idempotencyKey: $"ChildFunction{Guid.NewGuid()}")
        );

        parent = functionsRegistry.RegisterFunc(
            parentFunctionId.Type,
            inner: async Task<List<string>> (string param, Workflow workflow) =>
            {
                await workflow.Effect.Capture(async () =>
                {
                    for (var i = 0; i < numberOfChildren; i++)
                        await child.Schedule($"SomeChildInstance#{i}", i.ToString(), detach: true);
                });

                var messages = new List<string>();
                for (var i = 0; i < numberOfChildren; i++)
                {
                    var msg = await workflow.Message<string>();
                    messages.Add(msg);
                }
                
                return messages;
            }
        );

        await parent.Schedule(parentFunctionId.Instance.Value, "hello world");

        var controlPanel = await parent.ControlPanel(parentFunctionId.Instance);
        controlPanel.ShouldNotBeNull();
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        }, maxWait: TimeSpan.FromSeconds(60));

        var result = controlPanel.Result!.ToHashSet();
        for (var i = 0; i < numberOfChildren; i++)
            result.Contains(i.ToString()).ShouldBeTrue();
    }
    
    public abstract Task ParentCanWaitForBulkScheduledChildren();
    protected async Task ParentCanWaitForBulkScheduledChildren(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parentId = new FlowId($"ParentFlow{Guid.NewGuid()}", Guid.NewGuid().ToString());
        using var functionsRegistry = new FunctionsRegistry(store);
        
        var child = functionsRegistry.RegisterFunc(
            flowType: $"ChildFlow{Guid.NewGuid()}",
            inner: (string param) => param.ToUpper().ToTask()
        );

        var parent = functionsRegistry.RegisterFunc(
            parentId.Type,
            inner: async Task<string> (string param) =>
            {
                var results = await child.BulkSchedule(
                    param.Split(" ").Select((s, i) => new BulkWork<string>(i.ToString(), s))
                ).Completion();

                return results.StringJoin(" ");
            }
        );

        var result = await parent.Schedule(parentId.Instance, "hello world and universe").Completion();
        result.ShouldBe("HELLO WORLD AND UNIVERSE");
    }
    
    public abstract Task ChildIsCreatedWithParentsId();
    protected async Task ChildIsCreatedWithParentsId(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parentId = new FlowId($"ParentFlow{Guid.NewGuid()}", Guid.NewGuid().ToString());
        using var functionsRegistry = new FunctionsRegistry(store);

        FuncRegistration<string, string>? parent = null;
        var child = functionsRegistry.RegisterFunc(
            flowType: $"ChildFlow{Guid.NewGuid()}",
            inner: (string param) => param.ToUpper().ToTask()
        );

        parent = functionsRegistry.RegisterFunc(
            parentId.Type,
            inner: async Task<string> (string param) =>
                await child.Schedule("Child", param).Completion()
        );

        var result = await parent.Schedule(parentId.Instance, "hello world").Completion();
        result.ShouldBe("HELLO WORLD");
        
        var childStoredFunction = await store
            .GetFunction(StoredId.Create(child.StoredType, "Child"))
            .ShouldNotBeNullAsync();

        var parentStoredId = parentId.ToStoredId(parent.StoredType);
        childStoredFunction.ParentId.ShouldBe(parentStoredId);
    }
    
    public abstract Task SuspendedFlowIsRestartedAfterInterrupt();
    protected async Task SuspendedFlowIsRestartedAfterInterrupt(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();
        var (flowType, flowInstance) = id;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var syncFlag = new SyncedFlag();
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: Task (workflow) =>
            {
                if (syncFlag.IsRaised)
                    return Task.CompletedTask;
                
                syncFlag.Raise();
                throw new SuspendInvocationException();
            }
        );
        
        await registration.Schedule(flowInstance);
        await syncFlag.WaitForRaised();

        var controlPanel = await registration.ControlPanel(flowInstance).ShouldNotBeNullAsync();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Suspended;
        });

        await store.Interrupt(registration.MapToStoredId(id.Instance));
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExecutingFlowIsReExecutedWhenSuspendedAfterInterrupt();
    protected async Task ExecutingFlowIsReExecutedWhenSuspendedAfterInterrupt(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();
        var (flowType, flowInstance) = id;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var insideFlowFlag = new SyncedFlag();
        var canContinueFlag = new SyncedFlag();
        var executingAgainFlag = new SyncedFlag();
        
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async Task (workflow) =>
            {
                if (insideFlowFlag.IsRaised)
                {
                    executingAgainFlag.Raise();
                    return;
                }

                insideFlowFlag.Raise();
                await canContinueFlag.WaitForRaised();
                throw new SuspendInvocationException();
            }
        );
        
        await registration.Schedule(flowInstance);
        await insideFlowFlag.WaitForRaised();

        await store.Interrupt(registration.MapToStoredId(id.Instance));
        canContinueFlag.Raise();
        
        await executingAgainFlag.WaitForRaised();
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task InterruptSuspendedFlows();
    protected async Task InterruptSuspendedFlows(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();
        var (flowType, flowInstance) = id;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var succeedFlag = new SyncedFlag();
        
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: Task () =>
            {
                if (succeedFlag.IsRaised)
                    return Task.CompletedTask;
                
                throw new SuspendInvocationException();
            }
        );

        var flows = new [] { "Flow#1", "Flow#2", "Flow#3" }.Select(instance => new FlowInstance(instance)).ToList();
        await registration.BulkSchedule(flows);

        foreach (var flow in flows)
            await (await registration.ControlPanel(flow))!.BusyWaitUntil(cp => cp.Status == Status.Suspended);
        
        succeedFlag.Raise();

        await registration.Interrupt(flows.Select(f => f.Value.ToStoredId(registration.StoredType)));
        
        foreach (var flow in flows)
            await (await registration.ControlPanel(flow))!.BusyWaitUntil(cp => cp.Status == Status.Succeeded);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task AwaitMessageAfterAppendShouldNotCauseSuspension();
    protected async Task AwaitMessageAfterAppendShouldNotCauseSuspension(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();
        var (flowType, flowInstance) = id;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );
        
        var registration = functionsRegistry.RegisterFunc<string, string>(
            flowType,
            inner: async Task<string> (param, workflow) =>
            {
                await workflow.AppendMessage(param);
                    
                return await workflow.Message<string>();
            }
        );

        var result = await registration.Invoke("SomeInstance", "Hello World");
        result.ShouldBe("Hello World");
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task DelayedFlowIsRestartedOnce();
    protected async Task DelayedFlowIsRestartedOnce(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();
        var (flowType, flowInstance) = id;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );
        
        var syncedCounter = new SyncedCounter();
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async Task (workflow) =>
            {
                syncedCounter.Increment();
                await workflow.Delay(TimeSpan.FromMilliseconds(100));
            }
        );

        await registration.Schedule(flowInstance);
        
        var cp = await registration.ControlPanel(flowInstance);
        cp.ShouldNotBeNull();

        await cp.WaitForCompletion(allowPostponeAndSuspended: true);
        await cp.Refresh();
        
        cp.Status.ShouldBe(Status.Succeeded);
        syncedCounter.Current.ShouldBe(2);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task InterruptedExecutingFlowIsRestartedOnce();
    protected async Task InterruptedExecutingFlowIsRestartedOnce(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();
        var (flowType, flowInstance) = id;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );
        
        var syncedCounter = new SyncedCounter();
        var insideFlow = new SyncedFlag();
        var continueFlow = new SyncedFlag();
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async Task (workflow) =>
            {
                syncedCounter.Increment();
                insideFlow.Raise();
                if (syncedCounter.Current == 1)
                {
                    await continueFlow.WaitForRaised();
                    throw new SuspendInvocationException();
                }
            }
        );

        await registration.Schedule(flowInstance);
        await insideFlow.WaitForRaised();
        await registration.Interrupt([flowInstance]);
        continueFlow.Raise();
        
        var cp = await registration.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await cp.WaitForCompletion(allowPostponeAndSuspended: true);
        
        syncedCounter.Current.ShouldBe(2);

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task TwoDelaysFlowCompletesSuccessfully();
    protected async Task TwoDelaysFlowCompletesSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();
        var (flowType, flowInstance) = id;

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var counter = new SyncedCounter();
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async Task (workflow) =>
            {
                counter.Increment();
                await workflow.Delay(TimeSpan.FromMilliseconds(100), alias: "first");
                await workflow.Delay(TimeSpan.FromMilliseconds(100), alias: "second");
            }
        );

        await registration.Schedule(flowInstance);

        var cp = await registration.ControlPanel(flowInstance);
        cp.ShouldNotBeNull();
        
        await cp.WaitForCompletion(allowPostponeAndSuspended: true);
        await cp.Refresh();

        cp.Status.ShouldBe(Status.Succeeded);

        counter.Current.ShouldBe(3);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
}
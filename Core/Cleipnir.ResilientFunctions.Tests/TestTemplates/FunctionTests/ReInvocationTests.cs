using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class ReInvocationTests
{
    public abstract Task ActionReInvocationSunshineScenario();
    protected async Task ActionReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );
        var syncedParameter = new Synced<string>();

        var rFunc = functionsRegistry
            .RegisterAction(
                functionType, Task (string s) =>
                {
                    if (flag.Position == FlagPosition.Lowered)
                    {
                        flag.Raise();
                        throw new InvalidOperationException("oh no");
                    }

                    syncedParameter.Value = s;
                    return Task.CompletedTask;
                }
            );

        await Should.ThrowAsync<Exception>(() => rFunc.Run("something", "something"));

        await rFunc.ControlPanel("something").Result!.ScheduleRestart().Completion();
        
        syncedParameter.Value.ShouldBe("something");

        var function = await store.GetFunction(rFunc.MapToStoredId("something"));
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);

        var fwe = (FatalWorkflowException) unhandledExceptionCatcher.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
    }

    public abstract Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario();
    protected async Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );

        var syncedParam = new Synced<string>();
        var rAction = functionsRegistry.RegisterAction<string>(
            flowType,
            param =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    flag.Raise();
                    throw new InvalidOperationException("oh no");
                }

                syncedParam.Value = param;
                return Task.CompletedTask;
            }
        );

        await Should.ThrowAsync<Exception>(() =>
            rAction.Run(flowInstance.Value, "something")
        );
        
        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Param.ShouldBe("something");
        controlPanel.Param = "something_else";
        await controlPanel.SaveChanges();
       
        controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.ScheduleRestart().Completion();
        
        syncedParam.Value.ShouldBe("something_else");
        var fwe = (FatalWorkflowException) unhandledExceptionCatcher.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
    }
    
    public abstract Task UpdatedParameterAndStateIsPassedInOnReInvocationSunshineScenario();
    protected async Task UpdatedParameterAndStateIsPassedInOnReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );

        var syncedValue = new Synced<string>();
        var rAction = functionsRegistry.RegisterAction<string>(
            flowType,
            (param, workflow) =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    flag.Raise();
                    throw new InvalidOperationException("oh no");
                }

                syncedValue.Value = param;
                return Task.CompletedTask;
            }
        );

        await Should.ThrowAsync<Exception>(() =>
            rAction.Run(flowInstance.Value, "something")
        );

        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        controlPanel.Param.ShouldBe("something");
        controlPanel.Param = "something_else";
        await controlPanel.SaveChanges();
       
        controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.ScheduleRestart().Completion();
        
        syncedValue.Value.ShouldBe("something_else");

        var fwe = (FatalWorkflowException) unhandledExceptionCatcher.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
    }

    public abstract Task FuncReInvocationSunshineScenario();
    protected async Task FuncReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );

        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            flowType,
            async s =>
            {
                await Task.CompletedTask;
                if (flag.Position == FlagPosition.Lowered)
                {
                    flag.Raise();
                    throw new InvalidOperationException("oh no");
                }
                return s;
            }
        );

        await Should.ThrowAsync<Exception>(() => rFunc.Run(flowInstance.Value, "something"));

        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.ScheduleRestart().Completion();

        var storedId = rFunc.MapToStoredId(functionId.Instance);
        var function = await store.GetFunction(storedId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var results = await store.GetResults([storedId]);
        var resultBytes = results[storedId];
        resultBytes!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("something");

        var fwe = (FatalWorkflowException) unhandledExceptionCatcher.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
    }

    public abstract Task ReInvocationFailsWhenTheFunctionDoesNotExist();
    protected async Task ReInvocationFailsWhenTheFunctionDoesNotExist(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.Zero,
                enableWatchdogs: false
            )
        );

        var rAction = functionsRegistry.RegisterAction(
            flowType,
            (string _) => Task.CompletedTask
        );

        await rAction.Run(flowInstance.Value, "");
        var controlPanel1 = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var controlPanel2 = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel1.Delete();
        
        await Should.ThrowAsync<UnexpectedStateException>(() => controlPanel2.ScheduleRestart().Completion());

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
}
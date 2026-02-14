using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class ScheduleReInvocationTests
{
    public abstract Task ActionReInvocationSunshineScenario();
    protected async Task ActionReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionType = TestFlowId.Create().Type;
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
                functionType,
                inner: async (string s) =>
                {
                    await Task.CompletedTask;
                    if (flag.Position == FlagPosition.Lowered)
                    {
                        flag.Raise();
                        throw new InvalidOperationException("oh no");
                    }

                    syncedParameter.Value = s;
                }
            );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke("something", "something"));

        await rFunc.ControlPanel("something").Result!.ScheduleRestart();

        var functionId = new FlowId(functionType, "something");
        await BusyWait.Until(
            () => store.GetFunction(rFunc.MapToStoredId(functionId.Instance)).Map(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedParameter.Value.ShouldBe("something");

        var function = await store.GetFunction(rFunc.MapToStoredId("something"));
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);

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

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke(flowInstance.Value, "something"));

        var controlPanel = await rFunc.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.ScheduleRestart();

        var storedId = rFunc.MapToStoredId(functionId.Instance);
        await BusyWait.Until(
            () => store.GetFunction(storedId).Map(sf => sf?.Status == Status.Succeeded)
        );

        var function = await store.GetFunction(storedId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var results = await store.GetResults([storedId]);
        var resultBytes = results[storedId];
        resultBytes!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("something");

        var fwe = (FatalWorkflowException) unhandledExceptionCatcher.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
    }
}
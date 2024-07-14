using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class StateTests
{
    private class State : FlowState
    {
        public string Value { get; set; } = "";
    }
    
    public abstract Task StateCanBeFetchedFromFuncRegistration();
    protected async Task StateCanBeFetchedFromFuncRegistration(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var funcRegistration = functionsRegistry.RegisterFunc(
            flowType,
            async Task<string> (string param, Workflow workflow) =>
            {
                var state = workflow.States.CreateOrGet<State>();
                state.Value = "SomeValue";
                await state.Save();
                
                return "";
            }
        );

        await funcRegistration.Invoke(flowInstance.Value, param: "");

        var state = await funcRegistration.GetState<State>(flowInstance);
        state.ShouldNotBeNull();
        state.Value.ShouldBe("SomeValue");

        await Should.ThrowAsync<InvalidOperationException>(async () =>
        {
            state.Value = "SomeOtherValue";
            await state.Save();
        });

        state = await funcRegistration.GetState<State>(flowInstance);
        state.ShouldNotBeNull();
        state.Value.ShouldBe("SomeValue");
    }
    
    public abstract Task StateCanBeFetchedFromActionRegistration();
    protected async Task StateCanBeFetchedFromActionRegistration(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var actionRegistration = functionsRegistry.RegisterAction(
            flowType,
            async Task (string param, Workflow workflow) =>
            {
                var state = workflow.States.CreateOrGet<State>();
                state.Value = "SomeValue";
                await state.Save();
            }
        );

        await actionRegistration.Invoke(flowInstance.Value, param: "");

        var state = await actionRegistration.GetState<State>(flowInstance);
        state.ShouldNotBeNull();
        state.Value.ShouldBe("SomeValue");

        await Should.ThrowAsync<InvalidOperationException>(async () =>
        {
            state.Value = "SomeOtherValue";
            await state.Save();
        });
        
        state = await actionRegistration.GetState<State>(flowInstance);
        state.ShouldNotBeNull();
        state.Value.ShouldBe("SomeValue");
    }
}
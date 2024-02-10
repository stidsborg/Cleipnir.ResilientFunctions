using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class InMemorySunshineTests
{
    #region Func

    // ** SYNC ** //
    [TestMethod]
    public async Task SyncFuncSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                callback
            )
        );
    }
    
    // ** SYNC W. WORKFLOW ** //
    [TestMethod]
    public async Task SyncFuncWithWorkflowSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                string(string param, Workflow workflow) => callback(param)
            )
        );
    }
        
    // ** ASYNC ** //
    [TestMethod]
    public async Task AsyncFuncSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<string>(string param) => Task.FromResult(callback(param))
            )
        );
    }
    
    
    // ** ASYNC W. WORKFLOW * //
    [TestMethod]
    public async Task AsyncFuncWithWorkflowSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<string>(string param, Workflow workflow)
                    => Task.FromResult(callback(param))
            )
        );
    }
    
    // ** SYNC W. RESULT ** //
    [TestMethod]
    public async Task SyncFuncWithResultSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Result<string>(string param) => callback(param)
            )
        );
    }    
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    [TestMethod]
    public async Task SyncFuncWithWorkflowAndResultSunshineTest()
    {
        await ExecuteFunc((functionsRegistry, callback)
            => functionsRegistry.RegisterFunc(
                functionTypeId: "",
                Result<string>(string param, Workflow workflow) 
                    => Result.SucceedWithValue(callback(param))
               )
            );
    }    
   
    // ** ASYNC W. RESULT ** //
    [TestMethod]
    public async Task AsyncFuncWithResultSunshineTest()
    {
        await ExecuteFunc((functionsRegistry, callback)
            => functionsRegistry.RegisterFunc(
                functionTypeId: "",
                Task<Result<string>>(string param) 
                    => Task.FromResult(Result.SucceedWithValue(callback(param)))
               )
            );
    }    

    // ** ASYNC W. RESULT AND WORKFLOW ** //   
    [TestMethod]
    public async Task AsyncFuncWithWorkflowAndResultSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<Result<string>>(string param, Workflow workflow)
                    => Task.FromResult(Result.SucceedWithValue(callback(param))))
            );
    }    

    private async Task ExecuteFunc(Func<FunctionsRegistry, Func<string, string>, FuncRegistration<string, string>> createRegistration)
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new FunctionsRegistry(store);

        var syncedParam = new Synced<string>();
        var toReturn = "returned";
        // ReSharper disable once AccessToModifiedClosure
        var registration = createRegistration(rFunctions, param =>
        {
            syncedParam.Value = param;
            return toReturn;
        });

        var returned = await registration.Invoke("id1", "hello world");
        returned.ShouldBe(toReturn);
        syncedParam.Value.ShouldBe("hello world");

        syncedParam = new Synced<string>();
        var controlPanel = await registration.ControlPanel("id1");
        controlPanel.ShouldNotBeNull();
        returned = await controlPanel.ReInvoke();
        returned.ShouldBe(toReturn);
        syncedParam.Value.ShouldBe("hello world");
        
        syncedParam = new Synced<string>();
        await registration.Schedule("id2", "hello universe");
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        returned = await registration.Invoke("id2", "hello universe");
        returned.ShouldBe(toReturn);
        
        syncedParam = new Synced<string>();
        controlPanel = await registration.ControlPanel("id2");
        controlPanel.ShouldNotBeNull();
        await controlPanel.ScheduleReInvoke();
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        returned = await registration.Invoke("id2", "hello universe");
        returned.ShouldBe(toReturn);
    }

    #endregion
    
    #region Func_with_State

    // ** SYNC ** //
    [TestMethod]
    public async Task SyncStateFuncSunshineTest()
    {
        await ExecuteStateFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                callback
            )
        );
    }
    
    // ** SYNC W. WORKFLOW ** //
    [TestMethod]
    public async Task SyncStateFuncWithWorkflowSunshineTest()
    {
        await ExecuteStateFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                string (string param, WorkflowState state, Workflow workflow) => callback(param, state)
            )
        );
    }
        
    // ** ASYNC ** //
    [TestMethod]
    public async Task AsyncStateFuncSunshineTest()
    {
        await ExecuteStateFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<string>(string param, WorkflowState state) => Task.FromResult(callback(param, state))
            )
        );
    }
    
    
    // ** ASYNC W. WORKFLOW * //
    [TestMethod]
    public async Task AsyncStateFuncWithWorkflowSunshineTest()
    {
        await ExecuteStateFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<string>(string param, WorkflowState state, Workflow workflow)
                    => Task.FromResult(callback(param, state))
            )
        );
    }
    
    // ** SYNC W. RESULT ** //
    [TestMethod]
    public async Task SyncStateFuncWithResultSunshineTest()
    {
        await ExecuteStateFunc((rFunctions, callback)
            => rFunctions.RegisterFunc<string, WorkflowState, string>(
                functionTypeId: "",
                Result<string> (param, state) => callback(param, state)
            )
        );
    }

    // ** SYNC W. RESULT AND WORKFLOW ** //
    [TestMethod]
    public async Task SyncStateFuncWithWorkflowAndResultSunshineTest()
    {
        await ExecuteStateFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Result<string>(string param, WorkflowState state, Workflow workflow)
                    => callback(param, state)
            )
        );
    }    
   
    // ** ASYNC W. RESULT ** //
    [TestMethod]
    public async Task AsyncStateFuncWithResultSunshineTest()
    {
        await ExecuteStateFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<Result<string>>(string param, WorkflowState state)
                    => Task.FromResult(Result.SucceedWithValue(callback(param, state)))
            )
        );
    }    

    // ** ASYNC W. RESULT AND WORKFLOW ** //   
    [TestMethod]
    public async Task AsyncStateFuncWithWorkflowAndResultSunshineTest()
    {
        await ExecuteStateFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<Result<string>>(string param, WorkflowState state, Workflow workflow)
                    => Task.FromResult(Result.SucceedWithValue(callback(param, state)))
            )
        );
    }    

    private async Task ExecuteStateFunc(Func<FunctionsRegistry, Func<string, WorkflowState, string>, FuncRegistration<string, WorkflowState, string>> createRegistration)
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new FunctionsRegistry(store);

        var syncedParam = new Synced<string>();
        var syncedState = new Synced<WorkflowState>();
        var toReturn = "returned";
        // ReSharper disable once AccessToModifiedClosure
        var registration = createRegistration(
            rFunctions,
            (param, state) =>
            {
                syncedParam.Value = param;
                syncedState.Value = state;
                return toReturn;
            });

        var returned = await registration.Invoke("id1", "hello world");
        returned.ShouldBe(toReturn);
        syncedParam.Value.ShouldBe("hello world");
        syncedState.Value.ShouldNotBeNull();

        syncedParam = new Synced<string>();
        syncedState = new Synced<WorkflowState>();
        var controlPanel = await registration.ControlPanel("id1").ShouldNotBeNullAsync();
        returned = await controlPanel.ReInvoke();
        returned.ShouldBe(toReturn);
        syncedParam.Value.ShouldBe("hello world");
        syncedState.Value.ShouldNotBeNull();
        
        syncedParam = new Synced<string>();
        syncedState = new Synced<WorkflowState>();
        await registration.Schedule("id2", "hello universe");
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        syncedState.Value.ShouldNotBeNull();
        returned = await registration.Invoke("id2", "hello universe");
        returned.ShouldBe(toReturn);
        
        syncedParam = new Synced<string>();
        syncedState = new Synced<WorkflowState>();
        controlPanel = await registration.ControlPanel("id2").ShouldNotBeNullAsync();
        await controlPanel.ScheduleReInvoke();
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        syncedState.Value.ShouldNotBeNull();
        returned = await registration.Invoke("id2", "hello universe");
        returned.ShouldBe(toReturn);
    }

    private class WorkflowState : Domain.WorkflowState { }
    
    #endregion
    
    #region Action

    // ** SYNC ** //
    [TestMethod]
    public async Task SyncActionSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                callback
            )
        );
    }
    
    // ** SYNC W. WORKFLOW ** //
    [TestMethod]
    public async Task SyncActionWithWorkflowSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
                => rFunctions.RegisterAction(
                    functionTypeId: "",
                    void(string param, Workflow workflow) => callback(param)
                )
        );
    }
        
    // ** ASYNC ** //
    [TestMethod]
    public async Task AsyncActionSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task (string param) =>
                {
                    callback(param);
                    return Task.CompletedTask;
                })
        );
    }
    
    
    // ** ASYNC W. WORKFLOW * //
    [TestMethod]
    public async Task AsyncActionWithWorkflowSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task (string param, Workflow workflow) =>
                {
                    callback(param);
                    return Task.CompletedTask;
                })
        );
    }
    
    // ** SYNC W. RESULT ** //
    [TestMethod]
    public async Task SyncActionWithResultSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Result(string param) => { callback(param); return Result.Succeed; }
            )
        );
    }    
    
    // ** SYNC W. RESULT AND WORKFLOW ** //
    [TestMethod]
    public async Task SyncActionWithWorkflowAndResultSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Result(string param, Workflow workflow) => { callback(param); return Result.Succeed; }
            )
        );
    }    
   
    // ** ASYNC W. RESULT ** //
    [TestMethod]
    public async Task AsyncActionWithResultSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task<Result> (string param) => { callback(param); return Task.FromResult(Result.Succeed); }
            )
        );
    }    

    // ** ASYNC W. RESULT AND WORKFLOW ** //   
    [TestMethod]
    public async Task AsyncActionWithWorkflowAndResultSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task<Result> (string param, Workflow workflow) => { callback(param); return Task.FromResult(Result.Succeed); }
            )
        );
    }    

    private async Task ExecuteAction(Func<FunctionsRegistry, Action<string>, ActionRegistration<string>> createRegistration)
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new FunctionsRegistry(store);

        var syncedParam = new Synced<string>();
        // ReSharper disable once AccessToModifiedClosure
        var registration = createRegistration(rFunctions, param => syncedParam.Value = param);

        await registration.Invoke("id1", "hello world");
        syncedParam.Value.ShouldBe("hello world");

        syncedParam = new Synced<string>();
        await registration.ControlPanel("id1").Result!.ReInvoke();
        syncedParam.Value.ShouldBe("hello world");
        
        syncedParam = new Synced<string>();
        await registration.Schedule("id2", "hello universe");
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        await registration.Invoke("id2", "hello universe");
        
        syncedParam = new Synced<string>();
        await registration.ControlPanel("id2").Result!.ScheduleReInvoke();
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
    }

    #endregion
    
    #region Action_with_State

    // ** SYNC ** //
    [TestMethod]
    public async Task SyncStateActionSunshineTest()
    {
        await ExecuteStateAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                void (string param, WorkflowState state) => callback(param, state)
            )
        );
    }
    
    // ** SYNC W. WORKFLOW ** //
    [TestMethod]
    public async Task SyncStateActionWithWorkflowSunshineTest()
    {
        await ExecuteStateAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                void (string param, WorkflowState state, Workflow workflow) => callback(param, state)
            )
        );
    }
        
    // ** ASYNC ** //
    [TestMethod]
    public async Task AsyncStateActionSunshineTest()
    {
        await ExecuteStateAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task (string param, WorkflowState state) =>
                {
                    callback(param, state);
                    return Task.CompletedTask;
                })
        );
    }
    
    
    // ** ASYNC W. WORKFLOW * //
    [TestMethod]
    public async Task AsyncStateActionWithWorkflowSunshineTest()
    {
        await ExecuteStateAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task (string param, WorkflowState state, Workflow workflow) =>
                {
                    callback(param, state);
                    return Task.CompletedTask;
                })
        );
    }
    
    // ** SYNC W. RESULT ** //
    [TestMethod]
    public async Task SyncStateActionWithResultSunshineTest()
    {
        await ExecuteStateAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Result(string param, WorkflowState state) =>
                {
                    callback(param, state);
                    return Result.Succeed;
                })
        );
    }

    // ** SYNC W. RESULT AND WORKFLOW ** //
    [TestMethod]
    public async Task SyncStateActionWithWorkflowAndResultSunshineTest()
    {
        await ExecuteStateAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Result(string param, WorkflowState state, Workflow workflow) =>
                {
                    callback(param, state);
                    return Result.Succeed;
                })
        );
    }    
   
    // ** ASYNC W. RESULT ** //
    [TestMethod]
    public async Task AsyncStateActionWithResultSunshineTest()
    {
        await ExecuteStateAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task<Result> (string param, WorkflowState state) =>
                {
                    callback(param, state);
                    return Task.FromResult(Result.Succeed);
                })
        );
    }    

    // ** ASYNC W. RESULT AND WORKFLOW ** //   
    [TestMethod]
    public async Task AsyncStateActionWithWorkflowAndResultSunshineTest()
    {
        await ExecuteStateAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task<Result> (string param, WorkflowState state, Workflow workflow) =>
                {
                    callback(param, state);
                    return Task.FromResult(Result.Succeed);
                })
        );
    }    

    private async Task ExecuteStateAction(Func<FunctionsRegistry, Action<string, WorkflowState>, ActionRegistration<string, WorkflowState>> createRegistration)
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new FunctionsRegistry(store);

        var syncedParam = new Synced<string>();
        var syncedState = new Synced<WorkflowState>();
        // ReSharper disable once AccessToModifiedClosure
        var registration = createRegistration(
            rFunctions,
            (param, state) =>
            {
                syncedParam.Value = param;
                syncedState.Value = state;
            });

        await registration.Invoke("id1", "hello world");
        syncedParam.Value.ShouldBe("hello world");
        syncedState.Value.ShouldNotBeNull();

        syncedParam = new Synced<string>();
        syncedState = new Synced<WorkflowState>();
        await registration.ControlPanel("id1").Result!.ReInvoke();
        syncedParam.Value.ShouldBe("hello world");
        syncedState.Value.ShouldNotBeNull();
        
        syncedParam = new Synced<string>();
        syncedState = new Synced<WorkflowState>();
        await registration.Schedule("id2", "hello universe");
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        syncedState.Value.ShouldNotBeNull();
        await registration.Invoke("id2", "hello universe");
        
        syncedParam = new Synced<string>();
        syncedState = new Synced<WorkflowState>();
        await registration.ControlPanel("id2").Result!.ScheduleReInvoke();
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        syncedState.Value.ShouldNotBeNull();
    }

    #endregion
}
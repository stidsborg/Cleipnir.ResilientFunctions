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
                flowType: "",
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
                flowType: "",
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
                flowType: "",
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
                flowType: "",
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
                flowType: "",
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
                flowType: "",
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
                flowType: "",
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
                flowType: "",
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
        returned = await controlPanel.Restart();
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
        await controlPanel.ScheduleRestart();
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        returned = await registration.Invoke("id2", "hello universe");
        returned.ShouldBe(toReturn);
    }

    #endregion
    
    #region Action

    // ** SYNC ** //
    [TestMethod]
    public async Task SyncActionSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                flowType: "",
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
                    flowType: "",
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
                flowType: "",
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
                flowType: "",
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
                flowType: "",
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
                flowType: "",
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
                flowType: "",
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
                flowType: "",
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

        syncedParam.Value = null;
        await registration.ControlPanel("id1").Result!.Restart();
        syncedParam.Value.ShouldBe("hello world");

        syncedParam.Value = null;
        await registration.Schedule("id2", "hello universe");
        await BusyWait.UntilAsync(() => syncedParam.Value != null, maxWait: TimeSpan.FromSeconds(10));
        syncedParam.Value.ShouldBe("hello universe");
        
        var controlPanel = await registration.ControlPanel("id2");
        controlPanel.ShouldNotBeNull();
        await controlPanel.WaitForCompletion();

        syncedParam.Value = null;
        await registration.ControlPanel("id2").Result!.ScheduleRestart();
        await BusyWait.UntilAsync(() => syncedParam.Value != null, maxWait: TimeSpan.FromSeconds(10));
        syncedParam.Value.ShouldBe("hello universe");
    }

    #endregion
}
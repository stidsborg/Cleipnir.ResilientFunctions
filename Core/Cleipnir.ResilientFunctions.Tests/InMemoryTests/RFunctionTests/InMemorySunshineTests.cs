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
    
    // ** ASYNC ** //
    [TestMethod]
    public async Task AsyncFuncSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                flowType: "",
                callback
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
                Task<string>(string param, Workflow _) => callback(param)
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
                    => Succeed.WithValue(callback(param).Result).ToTask()
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
                Task<Result<string>> (string param, Workflow _)
                    => Succeed.WithValue(callback(param).Result).ToTask()
            )
        );
    }    

    private async Task ExecuteFunc(Func<FunctionsRegistry, Func<string, Task<string>>, FuncRegistration<string, string>> createRegistration)
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new FunctionsRegistry(store);

        var syncedParam = new Synced<string>();
        var toReturn = "returned";
        // ReSharper disable once AccessToModifiedClosure
        var registration = createRegistration(rFunctions, param =>
        {
            syncedParam.Value = param;
            return toReturn.ToTask();
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
        await BusyWait.Until(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        returned = await registration.Invoke("id2", "hello universe");
        returned.ShouldBe(toReturn);
        
        syncedParam = new Synced<string>();
        controlPanel = await registration.ControlPanel("id2");
        controlPanel.ShouldNotBeNull();
        await controlPanel.ScheduleRestart();
        await BusyWait.Until(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        returned = await registration.Invoke("id2", "hello universe");
        returned.ShouldBe(toReturn);
    }

    #endregion
    
    #region Action
        
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
   
    // ** ASYNC W. RESULT ** //
    [TestMethod]
    public async Task AsyncActionWithResultSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                flowType: "",
                Task<Result<Unit>> (string param) => { callback(param); return Succeed.WithUnit.ToTask(); }
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
                Task<Result<Unit>> (string param, Workflow workflow) => { callback(param); return Succeed.WithUnit.ToTask(); }
            )
        );
    }    

    private async Task ExecuteAction(Func<FunctionsRegistry, Func<string, Task>, ActionRegistration<string>> createRegistration)
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new FunctionsRegistry(store);

        var syncedParam = new Synced<string>();
        // ReSharper disable once AccessToModifiedClosure
        var registration = createRegistration(rFunctions, param =>
        {
            syncedParam.Value = param;
            return Task.CompletedTask;
        });

        await registration.Invoke("id1", "hello world");
        syncedParam.Value.ShouldBe("hello world");

        syncedParam.Value = null;
        await registration.ControlPanel("id1").Result!.Restart();
        syncedParam.Value.ShouldBe("hello world");

        syncedParam.Value = null;
        await registration.Schedule("id2", "hello universe");
        await BusyWait.Until(() => syncedParam.Value != null, maxWait: TimeSpan.FromSeconds(10));
        syncedParam.Value.ShouldBe("hello universe");
        
        var controlPanel = await registration.ControlPanel("id2");
        controlPanel.ShouldNotBeNull();
        await controlPanel.WaitForCompletion();

        syncedParam.Value = null;
        await registration.ControlPanel("id2").Result!.ScheduleRestart();
        await BusyWait.Until(() => syncedParam.Value != null, maxWait: TimeSpan.FromSeconds(10));
        syncedParam.Value.ShouldBe("hello universe");
    }

    #endregion
}
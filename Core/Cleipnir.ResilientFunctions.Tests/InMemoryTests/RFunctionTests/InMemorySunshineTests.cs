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
    
    #region Func_with_Scrapbook

    // ** SYNC ** //
    [TestMethod]
    public async Task SyncScrapbookFuncSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                callback
            )
        );
    }
    
    // ** SYNC W. WORKFLOW ** //
    [TestMethod]
    public async Task SyncScrapbookFuncWithWorkflowSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                string (string param, Scrapbook scrapbook, Workflow workflow) => callback(param, scrapbook)
            )
        );
    }
        
    // ** ASYNC ** //
    [TestMethod]
    public async Task AsyncScrapbookFuncSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<string>(string param, Scrapbook scrapbook) => Task.FromResult(callback(param, scrapbook))
            )
        );
    }
    
    
    // ** ASYNC W. WORKFLOW * //
    [TestMethod]
    public async Task AsyncScrapbookFuncWithWorkflowSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<string>(string param, Scrapbook scrapbook, Workflow workflow)
                    => Task.FromResult(callback(param, scrapbook))
            )
        );
    }
    
    // ** SYNC W. RESULT ** //
    [TestMethod]
    public async Task SyncScrapbookFuncWithResultSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterFunc<string, Scrapbook, string>(
                functionTypeId: "",
                Result<string> (param, scrapbook) => callback(param, scrapbook)
            )
        );
    }

    // ** SYNC W. RESULT AND WORKFLOW ** //
    [TestMethod]
    public async Task SyncScrapbookFuncWithWorkflowAndResultSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Result<string>(string param, Scrapbook scrapbook, Workflow workflow)
                    => callback(param, scrapbook)
            )
        );
    }    
   
    // ** ASYNC W. RESULT ** //
    [TestMethod]
    public async Task AsyncScrapbookFuncWithResultSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<Result<string>>(string param, Scrapbook scrapbook)
                    => Task.FromResult(Result.SucceedWithValue(callback(param, scrapbook)))
            )
        );
    }    

    // ** ASYNC W. RESULT AND WORKFLOW ** //   
    [TestMethod]
    public async Task AsyncScrapbookFuncWithWorkflowAndResultSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<Result<string>>(string param, Scrapbook scrapbook, Workflow workflow)
                    => Task.FromResult(Result.SucceedWithValue(callback(param, scrapbook)))
            )
        );
    }    

    private async Task ExecuteScrapbookFunc(Func<FunctionsRegistry, Func<string, Scrapbook, string>, FuncRegistration<string, Scrapbook, string>> createRegistration)
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new FunctionsRegistry(store);

        var syncedParam = new Synced<string>();
        var syncedScrapbook = new Synced<Scrapbook>();
        var toReturn = "returned";
        // ReSharper disable once AccessToModifiedClosure
        var registration = createRegistration(
            rFunctions,
            (param, scrapbook) =>
            {
                syncedParam.Value = param;
                syncedScrapbook.Value = scrapbook;
                return toReturn;
            });

        var returned = await registration.Invoke("id1", "hello world");
        returned.ShouldBe(toReturn);
        syncedParam.Value.ShouldBe("hello world");
        syncedScrapbook.Value.ShouldNotBeNull();

        syncedParam = new Synced<string>();
        syncedScrapbook = new Synced<Scrapbook>();
        var controlPanel = await registration.ControlPanel("id1").ShouldNotBeNullAsync();
        returned = await controlPanel.ReInvoke();
        returned.ShouldBe(toReturn);
        syncedParam.Value.ShouldBe("hello world");
        syncedScrapbook.Value.ShouldNotBeNull();
        
        syncedParam = new Synced<string>();
        syncedScrapbook = new Synced<Scrapbook>();
        await registration.Schedule("id2", "hello universe");
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        syncedScrapbook.Value.ShouldNotBeNull();
        returned = await registration.Invoke("id2", "hello universe");
        returned.ShouldBe(toReturn);
        
        syncedParam = new Synced<string>();
        syncedScrapbook = new Synced<Scrapbook>();
        controlPanel = await registration.ControlPanel("id2").ShouldNotBeNullAsync();
        await controlPanel.ScheduleReInvoke();
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        syncedScrapbook.Value.ShouldNotBeNull();
        returned = await registration.Invoke("id2", "hello universe");
        returned.ShouldBe(toReturn);
    }

    private class Scrapbook : RScrapbook { }
    
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
    
    #region Action_with_Scrapbook

    // ** SYNC ** //
    [TestMethod]
    public async Task SyncScrapbookActionSunshineTest()
    {
        await ExecuteScrapbookAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                void (string param, Scrapbook scrapbook) => callback(param, scrapbook)
            )
        );
    }
    
    // ** SYNC W. WORKFLOW ** //
    [TestMethod]
    public async Task SyncScrapbookActionWithWorkflowSunshineTest()
    {
        await ExecuteScrapbookAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                void (string param, Scrapbook scrapbook, Workflow workflow) => callback(param, scrapbook)
            )
        );
    }
        
    // ** ASYNC ** //
    [TestMethod]
    public async Task AsyncScrapbookActionSunshineTest()
    {
        await ExecuteScrapbookAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task (string param, Scrapbook scrapbook) =>
                {
                    callback(param, scrapbook);
                    return Task.CompletedTask;
                })
        );
    }
    
    
    // ** ASYNC W. WORKFLOW * //
    [TestMethod]
    public async Task AsyncScrapbookActionWithWorkflowSunshineTest()
    {
        await ExecuteScrapbookAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task (string param, Scrapbook scrapbook, Workflow workflow) =>
                {
                    callback(param, scrapbook);
                    return Task.CompletedTask;
                })
        );
    }
    
    // ** SYNC W. RESULT ** //
    [TestMethod]
    public async Task SyncScrapbookActionWithResultSunshineTest()
    {
        await ExecuteScrapbookAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Result(string param, Scrapbook scrapbook) =>
                {
                    callback(param, scrapbook);
                    return Result.Succeed;
                })
        );
    }

    // ** SYNC W. RESULT AND WORKFLOW ** //
    [TestMethod]
    public async Task SyncScrapbookActionWithWorkflowAndResultSunshineTest()
    {
        await ExecuteScrapbookAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Result(string param, Scrapbook scrapbook, Workflow workflow) =>
                {
                    callback(param, scrapbook);
                    return Result.Succeed;
                })
        );
    }    
   
    // ** ASYNC W. RESULT ** //
    [TestMethod]
    public async Task AsyncScrapbookActionWithResultSunshineTest()
    {
        await ExecuteScrapbookAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task<Result> (string param, Scrapbook scrapbook) =>
                {
                    callback(param, scrapbook);
                    return Task.FromResult(Result.Succeed);
                })
        );
    }    

    // ** ASYNC W. RESULT AND WORKFLOW ** //   
    [TestMethod]
    public async Task AsyncScrapbookActionWithWORKFLOWAndResultSunshineTest()
    {
        await ExecuteScrapbookAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task<Result> (string param, Scrapbook scrapbook, Workflow workflow) =>
                {
                    callback(param, scrapbook);
                    return Task.FromResult(Result.Succeed);
                })
        );
    }    

    private async Task ExecuteScrapbookAction(Func<FunctionsRegistry, Action<string, Scrapbook>, RAction<string, Scrapbook>> createRegistration)
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new FunctionsRegistry(store);

        var syncedParam = new Synced<string>();
        var syncedScrapbook = new Synced<Scrapbook>();
        // ReSharper disable once AccessToModifiedClosure
        var registration = createRegistration(
            rFunctions,
            (param, scrapbook) =>
            {
                syncedParam.Value = param;
                syncedScrapbook.Value = scrapbook;
            });

        await registration.Invoke("id1", "hello world");
        syncedParam.Value.ShouldBe("hello world");
        syncedScrapbook.Value.ShouldNotBeNull();

        syncedParam = new Synced<string>();
        syncedScrapbook = new Synced<Scrapbook>();
        await registration.ControlPanel("id1").Result!.ReInvoke();
        syncedParam.Value.ShouldBe("hello world");
        syncedScrapbook.Value.ShouldNotBeNull();
        
        syncedParam = new Synced<string>();
        syncedScrapbook = new Synced<Scrapbook>();
        await registration.Schedule("id2", "hello universe");
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        syncedScrapbook.Value.ShouldNotBeNull();
        await registration.Invoke("id2", "hello universe");
        
        syncedParam = new Synced<string>();
        syncedScrapbook = new Synced<Scrapbook>();
        await registration.ControlPanel("id2").Result!.ScheduleReInvoke();
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        syncedScrapbook.Value.ShouldNotBeNull();
    }

    #endregion
}
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
    
    // ** SYNC W. CONTEXT ** //
    [TestMethod]
    public async Task SyncFuncWithContextSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                string(string param, Context context) => callback(param)
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
    
    
    // ** ASYNC W. CONTEXT * //
    [TestMethod]
    public async Task AsyncFuncWithContextSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<string>(string param, Context context)
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
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    [TestMethod]
    public async Task SyncFuncWithContextAndResultSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Result<string>(string param, Context context) 
                    => Result.SucceedWithValue(callback(param))
               )
            );
    }    
   
    // ** ASYNC W. RESULT ** //
    [TestMethod]
    public async Task AsyncFuncWithResultSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<Result<string>>(string param) 
                    => Task.FromResult(Result.SucceedWithValue(callback(param)))
               )
            );
    }    

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    [TestMethod]
    public async Task AsyncFuncWithContextAndResultSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<Result<string>>(string param, Context context)
                    => Task.FromResult(Result.SucceedWithValue(callback(param))))
            );
    }    

    private async Task ExecuteFunc(Func<RFunctions, Func<string, string>, RFunc<string, string>> createRegistration)
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);

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
        var controlPanel = await registration.ControlPanels.For("id1");
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
        controlPanel = await registration.ControlPanels.For("id2");
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
    
    // ** SYNC W. CONTEXT ** //
    [TestMethod]
    public async Task SyncScrapbookFuncWithContextSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                string (string param, Scrapbook scrapbook, Context context) => callback(param, scrapbook)
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
    
    
    // ** ASYNC W. CONTEXT * //
    [TestMethod]
    public async Task AsyncScrapbookFuncWithContextSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<string>(string param, Scrapbook scrapbook, Context context)
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

    // ** SYNC W. RESULT AND CONTEXT ** //
    [TestMethod]
    public async Task SyncScrapbookFuncWithContextAndResultSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Result<string>(string param, Scrapbook scrapbook, Context context)
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

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    [TestMethod]
    public async Task AsyncScrapbookFuncWithContextAndResultSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterFunc(
                functionTypeId: "",
                Task<Result<string>>(string param, Scrapbook scrapbook, Context context)
                    => Task.FromResult(Result.SucceedWithValue(callback(param, scrapbook)))
            )
        );
    }    

    private async Task ExecuteScrapbookFunc(Func<RFunctions, Func<string, Scrapbook, string>, RFunc<string, Scrapbook, string>> createRegistration)
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);

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
        var controlPanel = await registration.ControlPanel.For("id1").ShouldNotBeNullAsync();
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
        controlPanel = await registration.ControlPanel.For("id2").ShouldNotBeNullAsync();
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
    
    // ** SYNC W. CONTEXT ** //
    [TestMethod]
    public async Task SyncActionWithContextSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
                => rFunctions.RegisterAction(
                    functionTypeId: "",
                    void(string param, Context context) => callback(param)
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
    
    
    // ** ASYNC W. CONTEXT * //
    [TestMethod]
    public async Task AsyncActionWithContextSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task (string param, Context context) =>
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
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    [TestMethod]
    public async Task SyncActionWithContextAndResultSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Result(string param, Context context) => { callback(param); return Result.Succeed; }
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

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    [TestMethod]
    public async Task AsyncActionWithContextAndResultSunshineTest()
    {
        await ExecuteAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task<Result> (string param, Context context) => { callback(param); return Task.FromResult(Result.Succeed); }
            )
        );
    }    

    private async Task ExecuteAction(Func<RFunctions, Action<string>, RAction<string>> createRegistration)
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);

        var syncedParam = new Synced<string>();
        // ReSharper disable once AccessToModifiedClosure
        var registration = createRegistration(rFunctions, param => syncedParam.Value = param);

        await registration.Invoke("id1", "hello world");
        syncedParam.Value.ShouldBe("hello world");

        syncedParam = new Synced<string>();
        await registration.ControlPanels.For("id1").Result!.ReInvoke();
        syncedParam.Value.ShouldBe("hello world");
        
        syncedParam = new Synced<string>();
        await registration.Schedule("id2", "hello universe");
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        await registration.Invoke("id2", "hello universe");
        
        syncedParam = new Synced<string>();
        await registration.ControlPanels.For("id2").Result!.ScheduleReInvoke();
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
    
    // ** SYNC W. CONTEXT ** //
    [TestMethod]
    public async Task SyncScrapbookActionWithContextSunshineTest()
    {
        await ExecuteScrapbookAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                void (string param, Scrapbook scrapbook, Context context) => callback(param, scrapbook)
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
    
    
    // ** ASYNC W. CONTEXT * //
    [TestMethod]
    public async Task AsyncScrapbookActionWithContextSunshineTest()
    {
        await ExecuteScrapbookAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task (string param, Scrapbook scrapbook, Context context) =>
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

    // ** SYNC W. RESULT AND CONTEXT ** //
    [TestMethod]
    public async Task SyncScrapbookActionWithContextAndResultSunshineTest()
    {
        await ExecuteScrapbookAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Result(string param, Scrapbook scrapbook, Context context) =>
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

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    [TestMethod]
    public async Task AsyncScrapbookActionWithContextAndResultSunshineTest()
    {
        await ExecuteScrapbookAction((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task<Result> (string param, Scrapbook scrapbook, Context context) =>
                {
                    callback(param, scrapbook);
                    return Task.FromResult(Result.Succeed);
                })
        );
    }    

    private async Task ExecuteScrapbookAction(Func<RFunctions, Action<string, Scrapbook>, RAction<string, Scrapbook>> createRegistration)
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);

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
        await registration.ControlPanels.For("id1").Result!.ReInvoke();
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
        await registration.ControlPanels.For("id2").Result!.ScheduleReInvoke();
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        syncedScrapbook.Value.ShouldNotBeNull();
    }

    #endregion
}
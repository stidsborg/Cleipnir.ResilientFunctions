using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Invocation;
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
            => rFunctions.RegisterAction(
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
                => rFunctions.RegisterAction(
                    functionTypeId: "",
                    void(string param, Context context) => callback(param)
                )
        );
    }
        
    // ** ASYNC ** //
    [TestMethod]
    public async Task AsyncFuncSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
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
    public async Task AsyncFuncWithContextSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
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
    public async Task SyncFuncWithResultSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Result(string param) => { callback(param); return Result.Succeed; }
            )
        );
    }    
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    [TestMethod]
    public async Task SyncFuncWithContextAndResultSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Result(string param, Context context) => { callback(param); return Result.Succeed; }
            )
        );
    }    
   
    // ** ASYNC W. RESULT ** //
    [TestMethod]
    public async Task AsyncFuncWithResultSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task<Result> (string param) => { callback(param); return Task.FromResult(Result.Succeed); }
            )
        );
    }    

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    [TestMethod]
    public async Task AsyncFuncWithContextAndResultSunshineTest()
    {
        await ExecuteFunc((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task<Result> (string param, Context context) => { callback(param); return Task.FromResult(Result.Succeed); }
            )
        );
    }    

    private async Task ExecuteFunc(Func<RFunctions, Action<string>, RAction<string>> createRegistration)
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);

        var syncedParam = new Synced<string>();
        // ReSharper disable once AccessToModifiedClosure
        var registration = createRegistration(rFunctions, param => syncedParam.Value = param);

        await registration.Invoke("id1", "hello world");
        syncedParam.Value.ShouldBe("hello world");

        syncedParam = new Synced<string>();
        await registration.ReInvoke("id1", new [] { Status.Succeeded });
        syncedParam.Value.ShouldBe("hello world");
        
        syncedParam = new Synced<string>();
        await registration.Schedule("id2", "hello universe");
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        await registration.Invoke("id2", "hello universe");
        
        syncedParam = new Synced<string>();
        await registration.ScheduleReInvocation("id2", new [] { Status.Succeeded });
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
    }

    #endregion
    
    #region Func_with_Scrapbook

    // ** SYNC ** //
    [TestMethod]
    public async Task SyncScrapbookFuncSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                void (string param, Scrapbook scrapbook) => callback(param, scrapbook)
            )
        );
    }
    
    // ** SYNC W. CONTEXT ** //
    [TestMethod]
    public async Task SyncScrapbookFuncWithContextSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                void (string param, Scrapbook scrapbook, Context context) => callback(param, scrapbook)
            )
        );
    }
        
    // ** ASYNC ** //
    [TestMethod]
    public async Task AsyncScrapbookFuncSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
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
    public async Task AsyncScrapbookFuncWithContextSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
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
    public async Task SyncScrapbookFuncWithResultSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
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
    public async Task SyncScrapbookFuncWithContextAndResultSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
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
    public async Task AsyncScrapbookFuncWithResultSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
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
    public async Task AsyncScrapbookFuncWithContextAndResultSunshineTest()
    {
        await ExecuteScrapbookFunc((rFunctions, callback)
            => rFunctions.RegisterAction(
                functionTypeId: "",
                Task<Result> (string param, Scrapbook scrapbook, Context context) =>
                {
                    callback(param, scrapbook);
                    return Task.FromResult(Result.Succeed);
                })
        );
    }    

    private async Task ExecuteScrapbookFunc(Func<RFunctions, Action<string, Scrapbook>, RAction<string, Scrapbook>> createRegistration)
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
        await registration.ReInvoke("id1", new [] { Status.Succeeded });
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
        await registration.ScheduleReInvocation("id2", new [] { Status.Succeeded });
        await BusyWait.UntilAsync(() => syncedParam.Value != null);
        syncedParam.Value.ShouldBe("hello universe");
        syncedScrapbook.Value.ShouldNotBeNull();
    }

    private class Scrapbook : RScrapbook { }
    
    #endregion
}
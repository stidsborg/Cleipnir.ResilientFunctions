using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class RegisterWithExplicitReturnTests
{
    [TestMethod]
    public async Task FuncWithExplicitReturnIsInvokedSuccessfully()
    {
        using var rFunctions = new RFunctions(new InMemoryFunctionStore());
        var syncedParam = new Synced<string>();
        var rFunc = rFunctions.Func<string, string>(
            "FunctionTypeId".ToFunctionTypeId(),
            inner: async param =>
            {
                await Task.CompletedTask;
                syncedParam.Value = param;
                return Succeed.WithValue(param.ToUpper());
            }).Register().Invoke;

        var result = await rFunc("", "hello world");
        syncedParam.Value.ShouldBe("hello world");
        result.ShouldBe("HELLO WORLD");
    }
    
    [TestMethod]
    public async Task FuncWithScrapbookAndExplicitReturnIsInvokedSuccessfully()
    {
        using var rFunctions = new RFunctions(new InMemoryFunctionStore());
        var syncedParam = new Synced<string>();
        var rFunc = rFunctions.FuncWithScrapbook<string, Scrapbook, string>(
            "FunctionTypeId".ToFunctionTypeId(),
            inner: async (param, scrapbook) =>
            {
                await Task.CompletedTask;
                syncedParam.Value = param;
                return Succeed.WithValue(param.ToUpper());
            }).Register().Invoke;

        var result = await rFunc("", "hello world");
        syncedParam.Value.ShouldBe("hello world");
        result.ShouldBe("HELLO WORLD");
    }
    
    [TestMethod]
    public async Task ActionWithExplicitReturnIsInvokedSuccessfully()
    {
        using var rFunctions = new RFunctions(new InMemoryFunctionStore());
        var syncedParam = new Synced<string>();
        var rAction = rFunctions
            .Action<string>(
                "FunctionTypeId".ToFunctionTypeId(),
                inner: async param =>
                {
                    await Task.CompletedTask;
                    syncedParam.Value = param;
                    return Succeed.WithoutValue;
                })
            .Register()
            .Invoke;

        await rAction("", "hello world");
        syncedParam.Value.ShouldBe("hello world");
    }
    
    [TestMethod]
    public async Task ActionWithScrapbookAndExplicitReturnIsInvokedSuccessfully()
    {
        using var rFunctions = new RFunctions(new InMemoryFunctionStore());
        var syncedParam = new Synced<string>();
        var rAction = rFunctions
            .ActionWithScrapbook<string, Scrapbook>(
                "FunctionTypeId".ToFunctionTypeId(),
                inner: async (param, scrapbook) =>
                {
                    await Task.CompletedTask;
                    syncedParam.Value = param;
                    return Succeed.WithoutValue;
                })
            .Register()
            .Invoke;

        await rAction("", "hello world");
        syncedParam.Value.ShouldBe("hello world");
    }
    
    private class Scrapbook : RScrapbook {}
}
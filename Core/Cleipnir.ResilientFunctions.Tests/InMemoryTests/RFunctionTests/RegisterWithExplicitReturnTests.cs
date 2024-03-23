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
        using var rFunctions = new FunctionsRegistry(new InMemoryFunctionStore());
        var syncedParam = new Synced<string>();
        var rFunc = rFunctions.RegisterFunc<string, string>(
            "FunctionTypeId".ToFunctionTypeId(),
            inner: async param =>
            {
                await Task.CompletedTask;
                syncedParam.Value = param;
                return Succeed.WithValue(param.ToUpper());
            }).Invoke;

        var result = await rFunc("", "hello world");
        syncedParam.Value.ShouldBe("hello world");
        result.ShouldBe("HELLO WORLD");
    }
    
    [TestMethod]
    public async Task FuncWithStateAndExplicitReturnIsInvokedSuccessfully()
    {
        using var rFunctions = new FunctionsRegistry(new InMemoryFunctionStore());
        var syncedParam = new Synced<string>();
        var rFunc = rFunctions.RegisterFunc<string, string>(
            "FunctionTypeId".ToFunctionTypeId(),
            inner: async (param, state) =>
            {
                await Task.CompletedTask;
                
                syncedParam.Value = param;
                return Succeed.WithValue(param.ToUpper());
            }).Invoke;

        var result = await rFunc("", "hello world");
        syncedParam.Value.ShouldBe("hello world");
        result.ShouldBe("HELLO WORLD");
    }
    
    [TestMethod]
    public async Task ActionWithExplicitReturnIsInvokedSuccessfully()
    {
        using var rFunctions = new FunctionsRegistry(new InMemoryFunctionStore());
        var syncedParam = new Synced<string>();
        var rAction = rFunctions
            .RegisterAction<string>(
                "FunctionTypeId".ToFunctionTypeId(),
                inner: async param =>
                {
                    await Task.CompletedTask;
                    syncedParam.Value = param;
                    return Succeed.WithoutValue;
                })
            .Invoke;

        await rAction("", "hello world");
        syncedParam.Value.ShouldBe("hello world");
    }
    
    [TestMethod]
    public async Task ActionWithStateAndExplicitReturnIsInvokedSuccessfully()
    {
        using var rFunctions = new FunctionsRegistry(new InMemoryFunctionStore());
        var syncedParam = new Synced<string>();
        var rAction = rFunctions
            .RegisterAction<string>(
                "FunctionTypeId".ToFunctionTypeId(),
                inner: async (param, workflow) =>
                {
                    await Task.CompletedTask;
                    syncedParam.Value = param;
                    return Succeed.WithoutValue;
                })
            .Invoke;

        await rAction("", "hello world");
        syncedParam.Value.ShouldBe("hello world");
    }
}
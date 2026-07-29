using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class DuplicateRegistrationTests
{
    [TestMethod]
    public async Task ReRegistrationRFuncWithIncompatibleTypeThrowsException()
    {
        using var rFunctions = await FunctionsRegistry.CreateAndStart(new InMemoryFunctionStore());
        _ = rFunctions.RegisterFunc(
            "SomeFunctionType",
            Task<Result<string>>(string param) => Succeed.WithValue(param.ToUpper()).ToTask()
        );
        
        Should.Throw<InvalidCastException>(() =>
            _ = rFunctions.RegisterFunc(
                "SomeFunctionType",
                Task<Result<int>>(string param) => Succeed.WithValue(int.Parse(param)).ToTask()
            )
        );
    }
    
    [TestMethod]
    public async Task ReRegistrationRFuncSucceedsWhenArgumentsAreIdentical()
    {
        using var rFunctions = await FunctionsRegistry.CreateAndStart(new InMemoryFunctionStore());
        _ = rFunctions.RegisterFunc(
            "SomeFunctionType",
            Task<Result<string>>(string param) => Succeed.WithValue(param.ToUpper()).ToTask()
        );

        _ = rFunctions.RegisterFunc(
            "SomeFunctionType",
            Task<Result<string>> (string param) => Succeed.WithValue(param.ToUpper()).ToTask()
        );
    }
    
    [TestMethod]
    public async Task ReRegistrationRActionSucceedsWhenArgumentsAreIdentical()
    {
        using var rFunctions = await FunctionsRegistry.CreateAndStart(new InMemoryFunctionStore());
        _ = rFunctions.RegisterFunc(
            "SomeFunctionType",
            Task<Result<Unit>>(string _) => Succeed.WithUnit.ToTask()
        );

        _ = rFunctions.RegisterFunc(
            "SomeFunctionType",
            Task<Result<Unit>> (string _) => Succeed.WithUnit.ToTask()
        );
    }

    [TestMethod]
    public async Task ReRegistrationRActionWithIncompatibleTypeThrowsException()
    {
        using var rFunctions = await FunctionsRegistry.CreateAndStart(new InMemoryFunctionStore());
        _ = rFunctions.RegisterFunc(
            "SomeFunctionType",
            Task<Result<Unit>>(string _) => Succeed.WithUnit.ToTask()
        );

        Should.Throw<InvalidCastException>(() =>
            _ = rFunctions.RegisterFunc(
                "SomeFunctionType",
                Task<Result<Unit>>(int _) => Succeed.WithUnit.ToTask()
            )
        );
    }
    
    [TestMethod]
    public async Task ReRegistrationFromFuncToActionThrowsArgumentException()
    {
        using var rFunctions = await FunctionsRegistry.CreateAndStart(new InMemoryFunctionStore());
        _ = rFunctions.RegisterFunc(
            "SomeFunctionType",
            Task<Result<Unit>>(string _) => Succeed.WithUnit.ToTask()
        );

        Should.Throw<InvalidCastException>(() =>
            _ = rFunctions.RegisterAction(
                "SomeFunctionType",
                Task (int _) => Succeed.WithUnit.ToTask()
            )
        );
    }
}
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
    public void ReRegistrationRFuncWithIncompatibleTypeThrowsException()
    {
        using var rFunctions = new FunctionsRegistry(new InMemoryFunctionStore());
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
    public void ReRegistrationRFuncSucceedsWhenArgumentsAreIdentical()
    {
        using var rFunctions = new FunctionsRegistry(new InMemoryFunctionStore());
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
    public void ReRegistrationRActionSucceedsWhenArgumentsAreIdentical()
    {
        using var rFunctions = new FunctionsRegistry(new InMemoryFunctionStore());
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
    public void ReRegistrationRActionWithIncompatibleTypeThrowsException()
    {
        using var rFunctions = new FunctionsRegistry(new InMemoryFunctionStore());
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
    public void ReRegistrationFromFuncToActionThrowsArgumentException()
    {
        using var rFunctions = new FunctionsRegistry(new InMemoryFunctionStore());
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
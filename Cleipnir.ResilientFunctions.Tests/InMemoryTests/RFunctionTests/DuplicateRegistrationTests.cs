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
    public void ReRegistrationRFuncWithIncompatibleTypeThrowsInArgumentException()
    {
        using var rFunctions = new RFunctions(new InMemoryFunctionStore());
        _ = rFunctions.RegisterFunc(
            "SomeFunctionType",
            Task<Result<string>>(string param) => Succeed.WithValue(param.ToUpper()).ToTask()
        );

        Should.Throw<ArgumentException>(() =>
            _ = rFunctions.RegisterFunc(
                "SomeFunctionType",
                Task<Result<int>>(string param) => Succeed.WithValue(int.Parse(param)).ToTask()
            )
        );
    }
    
    [TestMethod]
    public void ReRegistrationRFuncWithSameTypeThrowsInArgumentException()
    {
        using var rFunctions = new RFunctions(new InMemoryFunctionStore());
        var rFunc1 = rFunctions.RegisterFunc(
            "SomeFunctionType",
            Task<Result<string>>(string param) => Succeed.WithValue(param.ToUpper()).ToTask()
        );

        var rFunc2 = rFunctions.RegisterFunc(
            "SomeFunctionType",
            Task<Result<string>>(string param) => Succeed.WithValue(param.ToUpper()).ToTask()
        );

        ReferenceEquals(rFunc1, rFunc2).ShouldBeTrue();
    }
    
    [TestMethod]
    public void ReRegistrationRActionWithIncompatibleTypeThrowsInArgumentException()
    {
        using var rFunctions = new RFunctions(new InMemoryFunctionStore());
        _ = rFunctions.RegisterFunc(
            "SomeFunctionType",
            Task<Result>(string _) => Succeed.WithoutValue.ToTask()
        );

        Should.Throw<ArgumentException>(() =>
            _ = rFunctions.RegisterFunc(
                "SomeFunctionType",
                Task<Result>(int _) => Succeed.WithoutValue.ToTask()
            )
        );
    }
    
    [TestMethod]
    public void ReRegistrationRActionWithSameTypeThrowsInArgumentException()
    {
        using var rFunctions = new RFunctions(new InMemoryFunctionStore());
        var rFunc1 = rFunctions.RegisterFunc(
            "SomeFunctionType",
            Task<Result>(string param) => Succeed.WithoutValue.ToTask()
        );

        var rFunc2 = rFunctions.RegisterFunc(
            "SomeFunctionType",
            Task<Result>(string param) => Succeed.WithoutValue.ToTask()
        );

        ReferenceEquals(rFunc1, rFunc2).ShouldBeTrue();
    }
}
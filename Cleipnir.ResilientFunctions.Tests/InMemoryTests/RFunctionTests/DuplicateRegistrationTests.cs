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
        _ = rFunctions.Register(
            "SomeFunctionType",
            Task<Return<string>>(string param) => Succeed.WithValue(param.ToUpper()).ToTask()
        );

        Should.Throw<ArgumentException>(() =>
            _ = rFunctions.Register(
                "SomeFunctionType",
                Task<Return<int>>(string param) => Succeed.WithValue(int.Parse(param)).ToTask()
            )
        );
    }
    
    [TestMethod]
    public void ReRegistrationRFuncWithSameTypeThrowsInArgumentException()
    {
        using var rFunctions = new RFunctions(new InMemoryFunctionStore());
        var rFunc1 = rFunctions.Register(
            "SomeFunctionType",
            Task<Return<string>>(string param) => Succeed.WithValue(param.ToUpper()).ToTask()
        );

        var rFunc2 = rFunctions.Register(
            "SomeFunctionType",
            Task<Return<string>>(string param) => Succeed.WithValue(param.ToUpper()).ToTask()
        );

        ReferenceEquals(rFunc1, rFunc2).ShouldBeTrue();
    }
    
    [TestMethod]
    public void ReRegistrationRActionWithIncompatibleTypeThrowsInArgumentException()
    {
        using var rFunctions = new RFunctions(new InMemoryFunctionStore());
        _ = rFunctions.Register<string, Return>(
            "SomeFunctionType",
            Task<Return>(string _) => Succeed.WithoutValue.ToTask()
        );

        Should.Throw<ArgumentException>(() =>
            _ = rFunctions.Register<int, Return>(
                "SomeFunctionType",
                Task<Return>(int _) => Succeed.WithoutValue.ToTask()
            )
        );
    }
    
    [TestMethod]
    public void ReRegistrationRActionWithSameTypeThrowsInArgumentException()
    {
        using var rFunctions = new RFunctions(new InMemoryFunctionStore());
        var rFunc1 = rFunctions.Register<string, Return>(
            "SomeFunctionType",
            Task<Return>(string param) => Succeed.WithoutValue.ToTask()
        );

        var rFunc2 = rFunctions.Register<string, Return>(
            "SomeFunctionType",
            Task<Return>(string param) => Succeed.WithoutValue.ToTask()
        );

        ReferenceEquals(rFunc1, rFunc2).ShouldBeTrue();
    }
}
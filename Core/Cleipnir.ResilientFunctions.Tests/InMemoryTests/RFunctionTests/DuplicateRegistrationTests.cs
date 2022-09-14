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
}
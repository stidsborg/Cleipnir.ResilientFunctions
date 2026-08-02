using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class RegistrationSealingTests
{
    [TestMethod]
    public async Task RegisteringFuncAfterStartThrowsException()
    {
        using var registry = await FunctionsRegistry.CreateAndStart(new InMemoryFunctionStore(), _ => { });

        Should.Throw<InvalidOperationException>(() =>
            _ = registry.RegisterFunc(
                "SomeFlowType".ToFlowType(),
                (string param) => param.ToUpper().ToTask()
            )
        );
    }

    [TestMethod]
    public async Task RegisteringActionAfterStartThrowsException()
    {
        using var registry = await FunctionsRegistry.CreateAndStart(new InMemoryFunctionStore(), _ => { });

        Should.Throw<InvalidOperationException>(() =>
            _ = registry.RegisterAction(
                "SomeFlowType".ToFlowType(),
                (string _) => Task.CompletedTask
            )
        );
    }

    [TestMethod]
    public async Task RegisteringParamlessAfterStartThrowsException()
    {
        using var registry = await FunctionsRegistry.CreateAndStart(new InMemoryFunctionStore(), _ => { });

        Should.Throw<InvalidOperationException>(() =>
            _ = registry.RegisterParamless(
                "SomeFlowType".ToFlowType(),
                () => Task.CompletedTask
            )
        );
    }

    // Re-registering a flow type already registered in the same setup delegate remains legal - the seal is about
    // registrations arriving after the watchdogs have started, not about duplicates.
    [TestMethod]
    public async Task ReRegisteringWithinSetupIsAllowed()
    {
        using var registry = await FunctionsRegistry.CreateAndStart(
            new InMemoryFunctionStore(),
            r =>
            {
                _ = r.RegisterFunc("SomeFlowType".ToFlowType(), (string param) => param.ToUpper().ToTask());
                _ = r.RegisterFunc("SomeFlowType".ToFlowType(), (string param) => param.ToUpper().ToTask());
            }
        );

        registry.ShouldNotBeNull();
    }

    [TestMethod]
    public async Task FlowRegisteredInSetupIsInvocable()
    {
        FuncRegistration<string, string> registration = null!;
        using var registry = await FunctionsRegistry.CreateAndStart(
            new InMemoryFunctionStore(),
            r => { registration = r.RegisterFunc("SomeFlowType".ToFlowType(), (string param) => param.ToUpper().ToTask()); }
        );

        var result = await registration.Run("someInstance".ToFlowInstance(), "hello");
        result.ShouldBe("HELLO");
    }
}

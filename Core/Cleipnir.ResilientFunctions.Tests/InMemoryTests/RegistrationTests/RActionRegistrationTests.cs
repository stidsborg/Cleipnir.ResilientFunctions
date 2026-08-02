using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RegistrationTests;

[TestClass]
public class RActionRegistrationTests
{
    private readonly FlowType _flowType = new FlowType("flowType");
    private const string flowInstance = "flowInstance";
    
    [TestMethod]
    public async Task ConstructedFuncInvokeCanBeCreatedAndInvoked()
    {
        ActionRegistration<string> registration = null!;
        using var rFunctions = await CreateRFunctions(
            r => registration = r.RegisterAction<string>(_flowType, InnerAction)
        );
        var rAction = registration.Run;

        await rAction(flowInstance, "hello world");
    }

    private Task InnerAction(string param) => Task.CompletedTask;

    private Task<FunctionsRegistry> CreateRFunctions(Action<FunctionsRegistry> setup)
        => FunctionsRegistry.CreateAndStart(new InMemoryFunctionStore(), setup);
}
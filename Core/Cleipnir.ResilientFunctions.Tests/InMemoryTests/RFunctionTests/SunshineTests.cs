using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class SunshineTests : TestTemplates.RFunctionTests.SunshineTests
{
    [TestMethod]
    public override Task SunshineScenarioFunc()
        => SunshineScenarioFunc(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task SunshineScenarioFuncWithInitialEvents()
        => SunshineScenarioFuncWithInitialEvents(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineScenarioActionWithInitialEvents()
        => SunshineScenarioActionWithInitialEvents(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineScenarioFuncWithScrapbook()
        => SunshineScenarioFuncWithScrapbook(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineScenarioAction()
        => SunshineScenarioAction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineScenarioActionWithScrapbook()
        => SunshineScenarioActionWithScrapbook(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineScenarioNullReturningFunc()
        => SunshineScenarioNullReturningFunc(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineScenarioNullReturningFuncWithScrapbook()
        => SunshineScenarioNullReturningFuncWithScrapbook(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SecondInvocationOnNullReturningFuncReturnsNullSuccessfully()
        => SecondInvocationOnNullReturningFuncReturnsNullSuccessfully(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task InvocationModeShouldBeDirectInSunshineScenario()
        => InvocationModeShouldBeDirectInSunshineScenario(FunctionStoreFactory.Create());
}
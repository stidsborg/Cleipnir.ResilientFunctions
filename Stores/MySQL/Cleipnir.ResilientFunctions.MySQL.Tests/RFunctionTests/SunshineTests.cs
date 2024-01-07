using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.RFunctionTests;

[TestClass]
public class SunshineTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.SunshineTests
{
    [TestMethod]
    public override Task SunshineScenarioFunc() 
        => SunshineScenarioFunc(FunctionStoreFactory.Create());
    
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
        => SecondInvocationOnNullReturningFuncReturnsNullSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task InvocationModeShouldBeDirectInSunshineScenario()
        => InvocationModeShouldBeDirectInSunshineScenario(FunctionStoreFactory.Create());
}
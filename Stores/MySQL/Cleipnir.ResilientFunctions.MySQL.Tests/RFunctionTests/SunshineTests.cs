using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.RFunctionTests;

[TestClass]
public class SunshineTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.SunshineTests
{
    [TestMethod]
    public override Task SunshineScenarioFunc() 
        => SunshineScenarioFunc(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task SunshineScenarioParamless()
        => SunshineScenarioParamless(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineScenarioParamlessWithResultReturnType()
        => SunshineScenarioParamlessWithResultReturnType(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineScenarioFuncWithState() 
        => SunshineScenarioFuncWithState(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineScenarioAction() 
        => SunshineScenarioAction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineScenarioActionWithState() 
        => SunshineScenarioActionWithState(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineScenarioNullReturningFunc()
        => SunshineScenarioNullReturningFunc(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineScenarioNullReturningFuncWithState()
        => SunshineScenarioNullReturningFuncWithState(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SecondInvocationOnNullReturningFuncReturnsNullSuccessfully()
        => SecondInvocationOnNullReturningFuncReturnsNullSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task InvocationModeShouldBeDirectInSunshineScenario()
        => InvocationModeShouldBeDirectInSunshineScenario(FunctionStoreFactory.Create());
}
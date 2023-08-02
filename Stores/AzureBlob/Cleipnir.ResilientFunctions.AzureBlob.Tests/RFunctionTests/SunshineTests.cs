using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class SunshineTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.SunshineTests
{
    [TestMethod]
    public override Task SunshineScenarioFunc() 
        => SunshineScenarioFunc(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SunshineScenarioFuncWithInitialEvents()
        => SunshineScenarioFuncWithInitialEvents(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SunshineScenarioActionWithInitialEvents()
        => SunshineScenarioActionWithInitialEvents(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task SunshineScenarioFuncWithScrapbook() 
        => SunshineScenarioFuncWithScrapbook(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SunshineScenarioAction() 
        => SunshineScenarioAction(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SunshineScenarioActionWithScrapbook() 
        => SunshineScenarioActionWithScrapbook(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SunshineScenarioNullReturningFunc()
        => SunshineScenarioNullReturningFunc(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SunshineScenarioNullReturningFuncWithScrapbook()
        => SunshineScenarioNullReturningFuncWithScrapbook(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SecondInvocationOnNullReturningFuncReturnsNullSuccessfully()
        => SecondInvocationOnNullReturningFuncReturnsNullSuccessfully(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task InvocationModeShouldBeDirectInSunshineScenario()
        => InvocationModeShouldBeDirectInSunshineScenario(FunctionStoreFactory.FunctionStoreTask);
}
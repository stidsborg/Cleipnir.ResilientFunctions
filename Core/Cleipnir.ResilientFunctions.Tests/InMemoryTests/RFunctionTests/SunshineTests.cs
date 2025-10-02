using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class SunshineTests : TestTemplates.FunctionTests.SunshineTests
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
    public override Task SunshineScenarioAction()
        => SunshineScenarioAction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineScenarioNullReturningFunc()
        => SunshineScenarioNullReturningFunc(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionIsRemovedAfterRetentionPeriod()
        => FunctionIsRemovedAfterRetentionPeriod(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task EffectsAreNotFetchedOnFirstInvocation()
        => EffectsAreNotFetchedOnFirstInvocation(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task FlowIdCanBeExtractedFromWorkflowInstance()
        => FlowIdCanBeExtractedFromWorkflowInstance(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FlowIdCanBeExtractedFromAmbientState()
        => FlowIdCanBeExtractedFromAmbientState(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FlowIdCanBeExtractedFromAmbientStateAfterSuspension()
        => FlowIdCanBeExtractedFromAmbientStateAfterSuspension(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FuncCanBeCreatedWithInitialState()
        => FuncCanBeCreatedWithInitialState(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ActionCanBeCreatedWithInitialState()
        => ActionCanBeCreatedWithInitialState(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ParamlessCanBeCreatedWithInitialState()
        => ParamlessCanBeCreatedWithInitialState(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ParamlessCanBeCreatedWithInitialStateContainedStartedButNotCompletedEffect()
        => ParamlessCanBeCreatedWithInitialStateContainedStartedButNotCompletedEffect(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ParamlessCanBeCreatedWithInitialFailedEffect()
        => ParamlessCanBeCreatedWithInitialFailedEffect(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionCanAcceptAndReturnOptionType()
        => FunctionCanAcceptAndReturnOptionType(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExecutingFunctionHasOwner()
        => ExecutingFunctionHasOwner(FunctionStoreFactory.Create());
}
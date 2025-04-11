using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class SuspensionTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.SuspensionTests
{
    [TestMethod]
    public override Task ActionCanBeSuspended()
        => ActionCanBeSuspended(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionCanBeSuspended()
        => FunctionCanBeSuspended(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded()
        => DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task EligibleSuspendedFunctionIsPickedUpByWatchdog()
        => EligibleSuspendedFunctionIsPickedUpByWatchdog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag()
        => SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleByWatchdog()
        => SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleByWatchdog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ParamlessFunctionWithPrefilledMessageCompletes()
        => ParamlessFunctionWithPrefilledMessageCompletes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PostponedFunctionIsResumedAfterEventIsAppendedToMessages()
        => PostponedFunctionIsResumedAfterEventIsAppendedToMessages(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task StartedParentCanWaitForChildActionCompletion()
        => StartedParentCanWaitForChildActionCompletion(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ChildCanReturnResultToParent()
        => ChildCanReturnResultToParent(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ParentCanWaitForChildAction() 
        => ParentCanWaitForChildAction(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ParentCanWaitForFailedChildAction()
        => ParentCanWaitForFailedChildAction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PublishFromChildActionStressTest()
        => PublishFromChildActionStressTest(FunctionStoreFactory.Create());

    public override Task ParentCanWaitForBulkScheduledChildren()
        => ParentCanWaitForBulkScheduledChildren(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ChildIsCreatedWithParentsId()
        => ChildIsCreatedWithParentsId(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task InterruptCountIsUpdatedWhenMaxWaitDetectsIt()
        => InterruptCountIsUpdatedWhenMaxWaitDetectsIt(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task SuspendedFlowIsRestartedAfterInterrupt()
        => SuspendedFlowIsRestartedAfterInterrupt(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExecutingFlowIsReExecutedWhenSuspendedAfterInterrupt()
        => ExecutingFlowIsReExecutedWhenSuspendedAfterInterrupt(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task InterruptSuspendedFlows()
        => InterruptSuspendedFlows(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task AwaitMessageAfterAppendShouldNotCauseSuspension()
        => AwaitMessageAfterAppendShouldNotCauseSuspension(FunctionStoreFactory.Create());
}
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class SuspensionTests : TestTemplates.RFunctionTests.SuspensionTests
{
    [TestMethod]
    public override Task ActionCanBeSuspended()
        => ActionCanBeSuspended(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionCanBeSuspended()
        => FunctionCanBeSuspended(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded()
        => DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task PostponedFunctionIsResumedAfterEventIsAppendedToMessages()
        => PostponedFunctionIsResumedAfterEventIsAppendedToMessages(
            FunctionStoreFactory.Create()
        );
    
    [TestMethod]
    public override Task EligibleSuspendedFunctionIsPickedUpByWatchdog()
        => EligibleSuspendedFunctionIsPickedUpByWatchdog(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag()
        => SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleByWatchdog()
        => SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleByWatchdog(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task ParamlessFunctionWithPrefilledMessageCompletes()
        => ParamlessFunctionWithPrefilledMessageCompletes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task StartedChildFuncInvocationPublishesResultSuccessfully()
        => StartedChildFuncInvocationPublishesResultSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task StartedChildActionInvocationPublishesResultSuccessfully()
        => StartedChildActionInvocationPublishesResultSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PublishFromChildActionStressTest()
        => PublishFromChildActionStressTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task InterruptCountIsUpdatedWhenMaxWaitDetectsIt()
        => InterruptCountIsUpdatedWhenMaxWaitDetectsIt(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SuspendedFlowIsRestartedAfterInterrupt()
        => SuspendedFlowIsRestartedAfterInterrupt(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExecutingFlowIsReExecutedWhenSuspendedAfterInterrupt()
        => ExecutingFlowIsReExecutedWhenSuspendedAfterInterrupt(FunctionStoreFactory.Create());
}
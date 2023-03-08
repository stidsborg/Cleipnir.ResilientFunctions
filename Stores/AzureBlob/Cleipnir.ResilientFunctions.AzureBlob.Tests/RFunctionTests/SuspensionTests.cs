using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class SuspensionTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.SuspensionTests
{
    [TestMethod]
    public override Task ActionCanBeSuspended()
        => ActionCanBeSuspended(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task FunctionCanBeSuspended()
        => FunctionCanBeSuspended(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded()
        => DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task EligibleSuspendedFunctionIsPickedUpByWatchdog()
        => EligibleSuspendedFunctionIsPickedUpByWatchdog(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag()
        => SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleAndWriteHasTrueBoolFlag(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleByWatchdog()
        => SuspendedFunctionIsAutomaticallyReInvokedWhenEligibleByWatchdog(FunctionStoreFactory.FunctionStoreTask);
}
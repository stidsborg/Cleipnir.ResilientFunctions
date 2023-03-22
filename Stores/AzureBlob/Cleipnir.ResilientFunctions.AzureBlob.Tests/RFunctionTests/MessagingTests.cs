using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class MessagingTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.MessagingTests
{
    [TestMethod]
    public override Task FunctionCompletesAfterAwaitedMessageIsReceived()
        => FunctionCompletesAfterAwaitedMessageIsReceived(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist()
        => FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task TimeoutEventCausesSuspendedFunctionToBeReInvoked()
        => TimeoutEventCausesSuspendedFunctionToBeReInvoked(FunctionStoreFactory.FunctionStoreTask);
}
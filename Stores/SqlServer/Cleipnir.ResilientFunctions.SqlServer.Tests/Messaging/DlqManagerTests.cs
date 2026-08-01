using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.Messaging;

[TestClass]
public class DlqManagerTests : Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates.DlqManagerTests
{
    [TestMethod]
    public override Task DeadLetteredMessagesCanBeRedriven()
        => DeadLetteredMessagesCanBeRedriven(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task AllDeadLetteredMessagesForFlowAreRedriven()
        => AllDeadLetteredMessagesForFlowAreRedriven(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task RedrivenMessageRetainsIdempotencyKeySenderAndReceiver()
        => RedrivenMessageRetainsIdempotencyKeySenderAndReceiver(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DeletedDeadLetteredMessagesAreNotRedelivered()
        => DeletedDeadLetteredMessagesAreNotRedelivered(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DeadLetteredMessagesCanBePagedThrough()
        => DeadLetteredMessagesCanBePagedThrough(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task RedrivingAndDeletingUnknownOrEmptyInputIsANoOp()
        => RedrivingAndDeletingUnknownOrEmptyInputIsANoOp(FunctionStoreFactory.Create());
}

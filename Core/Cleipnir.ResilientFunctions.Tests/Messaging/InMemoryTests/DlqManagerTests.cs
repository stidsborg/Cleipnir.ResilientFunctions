using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Tests.InMemoryTests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.InMemoryTests;

[TestClass]
public class DlqManagerTests : TestTemplates.DlqManagerTests
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

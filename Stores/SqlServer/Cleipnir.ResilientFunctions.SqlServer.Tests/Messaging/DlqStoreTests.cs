using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.Messaging;

[TestClass]
public class DlqStoreTests : Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates.DlqStoreTests
{
    [TestMethod]
    public override Task AppendedDlqMessagesCanBeFetchedAgain()
        => AppendedDlqMessagesCanBeFetchedAgain(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagesCanBePagedThroughUsingOffsetAndLimit()
        => MessagesCanBePagedThroughUsingOffsetAndLimit(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagesForProvidedStoredIdsAreFetched()
        => MessagesForProvidedStoredIdsAreFetched(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagesAtProvidedPositionsAreFetched()
        => MessagesAtProvidedPositionsAreFetched(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DeletedDlqMessagesAreRemoved()
        => DeletedDlqMessagesAreRemoved(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FetchingEmptyDeadLetterQueueReturnsEmptyList()
        => FetchingEmptyDeadLetterQueueReturnsEmptyList(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DeletingEmptyOrUnknownPositionsDoesNotThrow()
        => DeletingEmptyOrUnknownPositionsDoesNotThrow(FunctionStoreFactory.Create());
}

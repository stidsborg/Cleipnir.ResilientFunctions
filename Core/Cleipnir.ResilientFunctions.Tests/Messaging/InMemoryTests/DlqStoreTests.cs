using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Tests.InMemoryTests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.InMemoryTests;

[TestClass]
public class DlqStoreTests : TestTemplates.DlqStoreTests
{
    [TestMethod]
    public override Task AppendedDlqMessagesCanBeFetchedAgain()
        => AppendedDlqMessagesCanBeFetchedAgain(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagesForProvidedStoredIdsAreFetched()
        => MessagesForProvidedStoredIdsAreFetched(FunctionStoreFactory.Create());

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

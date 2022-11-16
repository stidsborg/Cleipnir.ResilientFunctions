using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.Messaging;

[TestClass]
public class EventStoreTests :  ResilientFunctions.Tests.Messaging.TestTemplates.EventStoreTests
{
    [TestMethod]
    public override Task AppendedMessagesCanBeFetchedAgain()
        => AppendedMessagesCanBeFetchedAgain(NoSql.AutoCreateAndInitializeEventStore());

    [TestMethod]
    public override Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain()
        => AppendedMessagesUsingBulkMethodCanBeFetchedAgain(NoSql.AutoCreateAndInitializeEventStore());
    
    [TestMethod]
    public override Task SkippedMessagesAreNotFetched()
        => SkippedMessagesAreNotFetched(NoSql.AutoCreateAndInitializeEventStore());

    [TestMethod]
    public override Task TruncatedEventSourceContainsNoEvents()
        => TruncatedEventSourceContainsNoEvents(NoSql.AutoCreateAndInitializeEventStore());

    [TestMethod]
    public override Task NoExistingEventSourceCanBeTruncated()
        => NoExistingEventSourceCanBeTruncated(NoSql.AutoCreateAndInitializeEventStore());
}
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.Messaging;

[TestClass]
public class EventStoreTests :  ResilientFunctions.Tests.Messaging.TestTemplates.EventStoreTests
{
    [TestMethod]
    public override Task AppendedMessagesCanBeFetchedAgain()
        => AppendedMessagesCanBeFetchedAgain(Sql.CreateAndInitializeEventStore());

    [TestMethod]
    public override Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain()
        => AppendedMessagesUsingBulkMethodCanBeFetchedAgain(Sql.CreateAndInitializeEventStore());
    
    [TestMethod]
    public override Task SkippedMessagesAreNotFetched()
        => SkippedMessagesAreNotFetched(Sql.CreateAndInitializeEventStore());

    [TestMethod]
    public override Task TruncatedEventSourceContainsNoEvents()
        => TruncatedEventSourceContainsNoEvents(Sql.CreateAndInitializeEventStore());

    [TestMethod]
    public override Task NoExistingEventSourceCanBeTruncated()
        => NoExistingEventSourceCanBeTruncated(Sql.CreateAndInitializeEventStore());

    [TestMethod]
    public override Task ExistingEventSourceCanBeReplacedWithProvidedEvents()
        => ExistingEventSourceCanBeReplacedWithProvidedEvents(Sql.CreateAndInitializeEventStore());

    [TestMethod]
    public override Task NonExistingEventSourceCanBeReplacedWithProvidedEvents()
        => NonExistingEventSourceCanBeReplacedWithProvidedEvents(Sql.CreateAndInitializeEventStore());
}
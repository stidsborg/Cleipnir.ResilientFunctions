using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.Messaging;

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
    
    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource(Sql.CreateAndInitializeEventStore());
    
    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion(Sql.CreateAndInitializeEventStore());

    [TestMethod]
    public override Task FetchNonExistingEventsSucceeds()
        => FetchNonExistingEventsSucceeds(Sql.CreateAndInitializeEventStore());

    [TestMethod]
    public override Task EventSubscriptionPublishesAppendedEvents()
        => EventSubscriptionPublishesAppendedEvents(Sql.CreateAndInitializeEventStore());
}
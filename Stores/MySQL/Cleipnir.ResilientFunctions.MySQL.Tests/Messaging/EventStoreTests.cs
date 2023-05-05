using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.Messaging;

[TestClass]
public class EventStoreTests :  ResilientFunctions.Tests.Messaging.TestTemplates.EventStoreTests
{
    [TestMethod]
    public override Task AppendedMessagesCanBeFetchedAgain()
        => AppendedMessagesCanBeFetchedAgain(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain()
        => AppendedMessagesUsingBulkMethodCanBeFetchedAgain(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task SkippedMessagesAreNotFetched()
        => SkippedMessagesAreNotFetched(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task TruncatedEventSourceContainsNoEvents()
        => TruncatedEventSourceContainsNoEvents(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task NoExistingEventSourceCanBeTruncated()
        => NoExistingEventSourceCanBeTruncated(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ExistingEventSourceCanBeReplacedWithProvidedEvents()
        => ExistingEventSourceCanBeReplacedWithProvidedEvents(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task NonExistingEventSourceCanBeReplacedWithProvidedEvents()
        => NonExistingEventSourceCanBeReplacedWithProvidedEvents(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FetchNonExistingEventsSucceeds()
        => FetchNonExistingEventsSucceeds(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task EventSubscriptionPublishesAppendedEvents()
        => EventSubscriptionPublishesAppendedEvents(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task EventSubscriptionPublishesFiltersOutEventsWithSameIdempotencyKeys()
        => EventSubscriptionPublishesFiltersOutEventsWithSameIdempotencyKeys(Sql.AutoCreateAndInitializeStore());
}
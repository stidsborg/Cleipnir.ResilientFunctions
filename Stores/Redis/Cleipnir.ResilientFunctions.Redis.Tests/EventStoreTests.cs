namespace Cleipnir.ResilientFunctions.Redis.Tests;

[TestClass]
public class EventStoreTests :  ResilientFunctions.Tests.Messaging.TestTemplates.EventStoreTests
{
    [TestMethod]
    public override Task AppendedMessagesCanBeFetchedAgain()
        => AppendedMessagesCanBeFetchedAgain(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain()
        => AppendedMessagesUsingBulkMethodCanBeFetchedAgain(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task EventsCanBeReplaced()
        => EventsCanBeReplaced(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task EventsAreReplacedWhenCountIsAsExpected()
        => EventsAreReplacedWhenCountIsAsExpected(FunctionStoreFactory.FunctionStoreTask);
    
    [TestMethod]
    public override Task EventsAreNotReplacedWhenCountIsNotAsExpected()
        => EventsAreNotReplacedWhenCountIsNotAsExpected(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task SkippedMessagesAreNotFetched()
        => SkippedMessagesAreNotFetched(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task TruncatedEventSourceContainsNoEvents()
        => TruncatedEventSourceContainsNoEvents(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task NoExistingEventSourceCanBeTruncated()
        => NoExistingEventSourceCanBeTruncated(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ExistingEventSourceCanBeReplacedWithProvidedEvents()
        => ExistingEventSourceCanBeReplacedWithProvidedEvents(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task NonExistingEventSourceCanBeReplacedWithProvidedEvents()
        => NonExistingEventSourceCanBeReplacedWithProvidedEvents(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task FetchNonExistingEventsSucceeds()
        => FetchNonExistingEventsSucceeds(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task EventSubscriptionPublishesAppendedEvents()
        => EventSubscriptionPublishesAppendedEvents(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task EventSubscriptionPublishesFiltersOutEventsWithSameIdempotencyKeys()
        => EventSubscriptionPublishesFiltersOutEventsWithSameIdempotencyKeys(FunctionStoreFactory.FunctionStoreTask);
}
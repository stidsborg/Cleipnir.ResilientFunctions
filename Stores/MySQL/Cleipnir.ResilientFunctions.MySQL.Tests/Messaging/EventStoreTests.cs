using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.Messaging;

[TestClass]
public class MessageStoreTests :  ResilientFunctions.Tests.Messaging.TestTemplates.MessageStoreTests
{
    [TestMethod]
    public override Task AppendedMessagesCanBeFetchedAgain()
        => AppendedMessagesCanBeFetchedAgain(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain()
        => AppendedMessagesUsingBulkMethodCanBeFetchedAgain(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventsCanBeReplaced()
        => EventsCanBeReplaced(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventsAreReplacedWhenCountIsAsExpected()
        => EventsAreReplacedWhenCountIsAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventsAreNotReplacedWhenCountIsNotAsExpected()
        => EventsAreNotReplacedWhenCountIsNotAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SkippedMessagesAreNotFetched()
        => SkippedMessagesAreNotFetched(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task TruncatedMessagesContainsNoEvents()
        => TruncatedMessagesContainsNoEvents(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NoExistingMessagesCanBeTruncated()
        => NoExistingMessagesCanBeTruncated(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingMessagesCanBeReplacedWithProvidedEvents()
        => ExistingMessagesCanBeReplacedWithProvidedEvents(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NonExistingMessagesCanBeReplacedWithProvidedEvents()
        => NonExistingMessagesCanBeReplacedWithProvidedEvents(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoMessages()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoMessages(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoMessagesUsingBulkInsertion()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoMessagesUsingBulkInsertion(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FetchNonExistingEventsSucceeds()
        => FetchNonExistingEventsSucceeds(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventSubscriptionPublishesAppendedEvents()
        => EventSubscriptionPublishesAppendedEvents(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventSubscriptionPublishesFiltersOutEventsWithSameIdempotencyKeys()
        => EventSubscriptionPublishesFiltersOutEventsWithSameIdempotencyKeys(FunctionStoreFactory.Create());
}
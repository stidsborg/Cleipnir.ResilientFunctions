using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests.Messaging;

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
    public override Task EventsAreNotReplacedWhenPositionIsNotAsExpected()
        => EventsAreNotReplacedWhenPositionIsNotAsExpected(FunctionStoreFactory.Create());

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
    
    [TestMethod]
    public override Task MaxPositionIsCorrectForAppendedMessages()
        => MaxPositionIsCorrectForAppendedMessages(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task AppendedMultipleMessagesAtOnceCanBeFetchedAgain()
        => AppendedMultipleMessagesAtOnceCanBeFetchedAgain(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task AppendedBatchedMessageCanBeFetchedAgain()
        => AppendedBatchedMessageCanBeFetchedAgain(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task AppendedBatchedMessagesWithPositionCanBeFetchedAgain()
        => AppendedBatchedMessagesWithPositionCanBeFetchedAgain(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagesForMultipleStoreIdsCanBeFetched()
        => MessagesForMultipleStoreIdsCanBeFetched(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DeleteMessagesRemovesMessagesAtSpecifiedPositions()
        => DeleteMessagesRemovesMessagesAtSpecifiedPositions(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DeleteMessagesDeletesSpecifiedMessages()
        => DeleteMessagesDeletesSpecifiedMessages(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DeleteMessagesWithNonExistentPositionsDoesNotThrow()
        => DeleteMessagesWithNonExistentPositionsDoesNotThrow(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DeleteMessagesWithEmptyPositionsDoesNotThrow()
        => DeleteMessagesWithEmptyPositionsDoesNotThrow(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DeleteMessagesOnlyAffectsSpecifiedStoredId()
        => DeleteMessagesOnlyAffectsSpecifiedStoredId(FunctionStoreFactory.Create());
}
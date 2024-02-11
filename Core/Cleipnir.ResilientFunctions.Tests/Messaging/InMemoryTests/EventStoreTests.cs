using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.InMemoryTests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.InMemoryTests;

[TestClass]
public class MessageStoreTests :  TestTemplates.MessageStoreTests
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
}
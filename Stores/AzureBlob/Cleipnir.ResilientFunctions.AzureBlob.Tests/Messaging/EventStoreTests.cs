using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.Messaging;

[TestClass]
public class EventStoreTests :  ResilientFunctions.Tests.Messaging.TestTemplates.EventStoreTests
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
    public override Task SkippedMessagesAreNotFetched()
        => SkippedMessagesAreNotFetched(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task TruncatedEventSourceContainsNoEvents()
        => TruncatedEventSourceContainsNoEvents(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NoExistingEventSourceCanBeTruncated()
        => NoExistingEventSourceCanBeTruncated(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingEventSourceCanBeReplacedWithProvidedEvents()
        => ExistingEventSourceCanBeReplacedWithProvidedEvents(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NonExistingEventSourceCanBeReplacedWithProvidedEvents()
        => NonExistingEventSourceCanBeReplacedWithProvidedEvents(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion(FunctionStoreFactory.Create());

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
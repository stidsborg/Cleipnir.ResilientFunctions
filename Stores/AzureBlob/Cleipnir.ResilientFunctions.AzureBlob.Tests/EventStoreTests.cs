using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests;

[TestClass]
public class EventStoreTests :  ResilientFunctions.Tests.Messaging.TestTemplates.EventStoreTests
{
    
    private static readonly BlobContainerClient BlobContainerClient = null!;
    private static readonly AzureBlobFunctionStore FunctionStore = null!;
    private static bool ShouldRun => Settings.ConnectionString != null; 
    
    static EventStoreTests()
    {
        if (!ShouldRun) return;
        
        var blobServiceClient = new BlobServiceClient(Settings.ConnectionString);
        FunctionStore = FunctionStoreFactory.CreateAndInitialize(prefix: nameof(StoreTests).ToLower()).GetAwaiter().GetResult();
        BlobContainerClient = blobServiceClient.GetBlobContainerClient(FunctionStore.ContainerName);
        
        BlobContainerClient.DeleteAllBlobs().GetAwaiter().GetResult();
    }
    
    [TestMethod]
    public override Task AppendedMessagesCanBeFetchedAgain()
        => AppendedMessagesCanBeFetchedAgain(FunctionStore.CastTo<IFunctionStore>().EventStore.ToTask());

    [TestMethod]
    public override Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain()
        => AppendedMessagesUsingBulkMethodCanBeFetchedAgain(FunctionStore.CastTo<IFunctionStore>().EventStore.ToTask());
    
    [TestMethod]
    public override Task SkippedMessagesAreNotFetched()
        => SkippedMessagesAreNotFetched(FunctionStore.CastTo<IFunctionStore>().EventStore.ToTask());

    [TestMethod]
    public override Task TruncatedEventSourceContainsNoEvents()
        => TruncatedEventSourceContainsNoEvents(FunctionStore.CastTo<IFunctionStore>().EventStore.ToTask());

    [TestMethod]
    public override Task NoExistingEventSourceCanBeTruncated()
        => NoExistingEventSourceCanBeTruncated(FunctionStore.CastTo<IFunctionStore>().EventStore.ToTask());

    [TestMethod]
    public override Task ExistingEventSourceCanBeReplacedWithProvidedEvents()
        => ExistingEventSourceCanBeReplacedWithProvidedEvents(FunctionStore.CastTo<IFunctionStore>().EventStore.ToTask());

    [TestMethod]
    public override Task NonExistingEventSourceCanBeReplacedWithProvidedEvents()
        => NonExistingEventSourceCanBeReplacedWithProvidedEvents(FunctionStore.CastTo<IFunctionStore>().EventStore.ToTask());

    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource(FunctionStore.CastTo<IFunctionStore>().EventStore.ToTask());

    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion(FunctionStore.CastTo<IFunctionStore>().EventStore.ToTask());

    [TestMethod]
    public override Task FetchNonExistingEventsSucceeds()
        => FetchNonExistingEventsSucceeds(FunctionStore.CastTo<IFunctionStore>().EventStore.ToTask());

    [TestMethod]
    public override Task EventSubscriptionPublishesAppendedEvents()
        => EventSubscriptionPublishesAppendedEvents(FunctionStore.CastTo<IFunctionStore>().EventStore.ToTask());
}
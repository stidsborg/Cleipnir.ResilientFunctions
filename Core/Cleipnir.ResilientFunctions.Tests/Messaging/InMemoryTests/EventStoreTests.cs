using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.InMemoryTests;

[TestClass]
public class EventStoreTests :  TestTemplates.EventStoreTests
{
    [TestMethod]
    public override Task AppendedMessagesCanBeFetchedAgain()
        => AppendedMessagesCanBeFetchedAgain(new InMemoryEventStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain()
        => AppendedMessagesUsingBulkMethodCanBeFetchedAgain(new InMemoryEventStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task SkippedMessagesAreNotFetched()
        => SkippedMessagesAreNotFetched(new InMemoryEventStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task TruncatedEventSourceContainsNoEvents()
        => TruncatedEventSourceContainsNoEvents(new InMemoryEventStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task NoExistingEventSourceCanBeTruncated()
        => NoExistingEventSourceCanBeTruncated(new InMemoryEventStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task ExistingEventSourceCanBeReplacedWithProvidedEvents()
        => ExistingEventSourceCanBeReplacedWithProvidedEvents(new InMemoryEventStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task NonExistingEventSourceCanBeReplacedWithProvidedEvents()
        => NonExistingEventSourceCanBeReplacedWithProvidedEvents(new InMemoryEventStore().CastTo<IEventStore>().ToTask());
}
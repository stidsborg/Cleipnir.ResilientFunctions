using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Messaging.Tests.InMemoryTests;

[TestClass]
public class EventStoreTests :  Messaging.Tests.TestTemplates.EventStoreTests
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

    public override Task NoExistingEventSourceCanBeTruncated()
        => NoExistingEventSourceCanBeTruncated(new InMemoryEventStore().CastTo<IEventStore>().ToTask());
}
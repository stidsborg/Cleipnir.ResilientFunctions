﻿using System.Threading.Tasks;
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
        => AppendedMessagesCanBeFetchedAgain(new InMemoryFunctionStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain()
        => AppendedMessagesUsingBulkMethodCanBeFetchedAgain(new InMemoryFunctionStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task SkippedMessagesAreNotFetched()
        => SkippedMessagesAreNotFetched(new InMemoryFunctionStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task TruncatedEventSourceContainsNoEvents()
        => TruncatedEventSourceContainsNoEvents(new InMemoryFunctionStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task NoExistingEventSourceCanBeTruncated()
        => NoExistingEventSourceCanBeTruncated(new InMemoryFunctionStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task ExistingEventSourceCanBeReplacedWithProvidedEvents()
        => ExistingEventSourceCanBeReplacedWithProvidedEvents(new InMemoryFunctionStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task NonExistingEventSourceCanBeReplacedWithProvidedEvents()
        => NonExistingEventSourceCanBeReplacedWithProvidedEvents(new InMemoryFunctionStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSource(new InMemoryFunctionStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion()
        => EventWithExistingIdempotencyKeyIsNotInsertedIntoEventSourceUsingBulkInsertion(new InMemoryFunctionStore().CastTo<IEventStore>().ToTask());
}
﻿namespace Cleipnir.ResilientFunctions.Messaging.SqlServer.Tests;

[TestClass]
public class EventStoreTests :  Messaging.Tests.TestTemplates.EventStoreTests
{
    [TestMethod]
    public override Task AppendedMessagesCanBeFetchedAgain()
        => AppendedMessagesCanBeFetchedAgain(Sql.CreateAndInitializeEventStore());

    [TestMethod]
    public override Task AppendedMessagesUsingBulkMethodCanBeFetchedAgain()
        => AppendedMessagesUsingBulkMethodCanBeFetchedAgain(Sql.CreateAndInitializeEventStore());
}
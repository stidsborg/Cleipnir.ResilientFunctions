using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Messaging.Tests.InMemoryTests;

[TestClass]
public class CustomEventSerializerTests : Cleipnir.ResilientFunctions.Messaging.Tests.TestTemplates.CustomEventSerializerTests
{
    [TestMethod]
    public override Task CustomEventSerializerIsUsedWhenSpecified()
        => CustomEventSerializerIsUsedWhenSpecified(new InMemoryEventStore().CastTo<IEventStore>().ToTask());
}
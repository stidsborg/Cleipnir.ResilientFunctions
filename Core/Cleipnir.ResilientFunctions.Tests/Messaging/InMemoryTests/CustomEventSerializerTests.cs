using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.InMemoryTests;

[TestClass]
public class CustomEventSerializerTests : TestTemplates.CustomEventSerializerTests
{
    [TestMethod]
    public override Task CustomEventSerializerIsUsedWhenSpecified()
        => CustomEventSerializerIsUsedWhenSpecified(new InMemoryEventStore().CastTo<IEventStore>().ToTask());
}
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.Messaging;

[TestClass]
public class CustomEventSerializerTests : ResilientFunctions.Tests.Messaging.TestTemplates.CustomEventSerializerTests
{
    [TestMethod]
    public override Task CustomEventSerializerIsUsedWhenSpecified()
        => CustomEventSerializerIsUsedWhenSpecified(Sql.CreateAndInitializeEventStore());
}
namespace Cleipnir.ResilientFunctions.Messaging.PostgreSQL.Tests;

[TestClass]
public class CustomEventSerializerTests : Cleipnir.ResilientFunctions.Messaging.Tests.TestTemplates.CustomEventSerializerTests
{
    [TestMethod]
    public override Task CustomEventSerializerIsUsedWhenSpecified()
        => CustomEventSerializerIsUsedWhenSpecified(Sql.CreateAndInitializeEventStore());
}
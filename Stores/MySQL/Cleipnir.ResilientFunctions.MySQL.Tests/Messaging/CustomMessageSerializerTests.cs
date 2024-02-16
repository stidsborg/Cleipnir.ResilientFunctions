using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.Messaging;

[TestClass]
public class CustomMessageSerializerTests : ResilientFunctions.Tests.Messaging.TestTemplates.CustomMessageSerializerTests
{
    [TestMethod]
    public override Task CustomEventSerializerIsUsedWhenSpecified()
        => CustomEventSerializerIsUsedWhenSpecified(FunctionStoreFactory.Create());
}
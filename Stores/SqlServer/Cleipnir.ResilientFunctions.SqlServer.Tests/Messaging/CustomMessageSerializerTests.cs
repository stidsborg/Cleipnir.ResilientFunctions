using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.Messaging;

[TestClass]
public class CustomMessageSerializerTests : Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates.CustomMessageSerializerTests
{
    [TestMethod]
    public override Task CustomEventSerializerIsUsedWhenSpecified()
        => CustomEventSerializerIsUsedWhenSpecified(FunctionStoreFactory.Create());
}
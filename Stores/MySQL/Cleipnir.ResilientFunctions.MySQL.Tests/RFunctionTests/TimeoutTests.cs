using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.RFunctionTests;

[TestClass]
public class TimeoutTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.TimeoutTests
{
    [TestMethod]
    public override Task ExpiredTimeoutIsAddedToMessages()
        => ExpiredTimeoutIsAddedToMessages(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExpiredTimeoutMakesReactiveChainThrowTimeoutException()
        => ExpiredTimeoutMakesReactiveChainThrowTimeoutException(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task RegisteredTimeoutIsCancelledAfterReactiveChainCompletes()
        => RegisteredTimeoutIsCancelledAfterReactiveChainCompletes(FunctionStoreFactory.Create());
}
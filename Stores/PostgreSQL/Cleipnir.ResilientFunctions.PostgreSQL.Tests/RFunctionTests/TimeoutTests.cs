using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class TimeoutTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.TimeoutTests
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

    [TestMethod]
    public override Task PendingTimeoutCanBeRemovedFromControlPanel()
        => PendingTimeoutCanBeRemovedFromControlPanel(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PendingTimeoutCanBeUpdatedFromControlPanel()
        => PendingTimeoutCanBeUpdatedFromControlPanel(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ProvidedUtcNowDelegateIsUsedInWatchdog()
        => ProvidedUtcNowDelegateIsUsedInWatchdog(FunctionStoreFactory.Create());
}
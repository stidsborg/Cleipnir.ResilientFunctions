using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class InMemoryDelayTest : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.InMemoryDelayTest
{
    [TestMethod]
    public override Task NonExpiredInMemoryDelayedInvocationIsNotPickedUpByWatchdogDespiteCrash()
        => NonExpiredInMemoryDelayedInvocationIsNotPickedUpByWatchdogDespiteCrash(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ExpiredInMemoryDelayedInvocationIsPickedUpByWatchdogOnCrash()
        => ExpiredInMemoryDelayedInvocationIsPickedUpByWatchdogOnCrash(Sql.AutoCreateAndInitializeStore());
}
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class InMemoryDelayTest : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.InMemoryDelayTest
{
    [TestMethod]
    public override Task NonExpiredInMemoryDelayedInvocationIsNotPickedUpByWatchdogDespiteCrash()
        => NonExpiredInMemoryDelayedInvocationIsNotPickedUpByWatchdogDespiteCrash(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );

    [TestMethod]
    public override Task ExpiredInMemoryDelayedInvocationIsPickedUpByWatchdogOnCrash()
        => ExpiredInMemoryDelayedInvocationIsPickedUpByWatchdogOnCrash(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );
}
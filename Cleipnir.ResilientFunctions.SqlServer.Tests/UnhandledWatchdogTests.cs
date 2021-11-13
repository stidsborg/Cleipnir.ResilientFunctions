using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests
{
    [TestClass]
    public class UnhandledWatchdogTests : Cleipnir.ResilientFunctions.Tests.UnhandledWatchdogTests
    {
        [TestMethod]
        public override Task UnhandledFunctionInvocationIsCompletedByWatchDog()
            => UnhandledFunctionInvocationIsCompletedByWatchDog(new InMemoryFunctionStore());
    }
}
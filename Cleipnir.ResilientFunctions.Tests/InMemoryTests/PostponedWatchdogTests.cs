using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class PostponedWatchdogTests : TestTemplates.PostponedWatchdogTests
    {
        [TestMethod]
        public override Task PostponedFunctionInvocationIsCompletedByWatchDog()
            => PostponedFunctionInvocationIsCompletedByWatchDog(new InMemoryFunctionStore());
    }
}
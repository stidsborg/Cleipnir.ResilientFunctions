using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class SuspensionTests : TestTemplates.RFunctionTests.SuspensionTests
{
    [TestMethod]
    public override Task ActionCanBeSuspended()
        => ActionCanBeSuspended(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task FunctionCanBeSuspended()
        => FunctionCanBeSuspended(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded()
        => DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );

}
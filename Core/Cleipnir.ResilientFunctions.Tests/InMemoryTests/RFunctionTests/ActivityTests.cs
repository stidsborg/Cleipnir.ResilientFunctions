using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class ActivityTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ActivityTests
{
    [TestMethod]
    public override Task SunshineActionTest()
        => SunshineActionTest(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task SunshineAsyncActionTest()
        => SunshineAsyncActionTest(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task SunshineFuncTest()
        => SunshineFuncTest(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task SunshineAsyncFuncTest()
        => SunshineAsyncFuncTest(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ExceptionThrowingActionTest()
        => ExceptionThrowingActionTest(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());
}
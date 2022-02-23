using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class UnhandledFuncExceptionTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.UnhandledFuncExceptionTests
{
    [TestMethod]
    public override Task UnhandledExceptionResultsInPostponedFunc()
        => UnhandledExceptionResultsInPostponedFunc(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task UnhandledExceptionResultsInPostponedFuncWithScrapbook()
        => UnhandledExceptionResultsInPostponedFuncWithScrapbook(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );

    [TestMethod]
    public override Task UnhandledExceptionResultsInPostponedAction()
        => UnhandledExceptionResultsInPostponedAction(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );

    [TestMethod]
    public override Task UnhandledExceptionResultsInPostponedActionWithScrapbook()
        => UnhandledExceptionResultsInPostponedActionWithScrapbook(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );
}
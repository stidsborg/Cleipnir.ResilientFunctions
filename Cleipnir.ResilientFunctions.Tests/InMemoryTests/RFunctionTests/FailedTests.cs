using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class FailedTests : TestTemplates.RFunctionTests.FailedTests
{
    [TestMethod]
    public override Task ExceptionThrowingFuncIsNotCompletedByWatchDog()
        => ExceptionThrowingFuncIsNotCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog()
        => ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task UnhandledExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ExceptionThrowingActionIsNotCompletedByWatchDog()
        => ExceptionThrowingActionIsNotCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task UnhandledExceptionThrowingActionIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingActionIsNotCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog()
        => ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task UnhandledExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());
}
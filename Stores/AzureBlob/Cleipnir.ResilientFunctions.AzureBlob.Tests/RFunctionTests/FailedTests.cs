using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class FailedTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.FailedTests
{
    [TestMethod]
    public override Task ExceptionThrowingFuncIsNotCompletedByWatchDog()
        => ExceptionThrowingFuncIsNotCompletedByWatchDog(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog()
        => ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(FunctionStoreFactory.FunctionStoreTask);

    public override Task UnhandledExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ExceptionThrowingActionIsNotCompletedByWatchDog()
        => ExceptionThrowingActionIsNotCompletedByWatchDog(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task PassingInNullParameterResultsInArgumentNullException()
        => PassingInNullParameterResultsInException(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog()
        => ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(FunctionStoreFactory.FunctionStoreTask);

    [TestMethod]
    public override Task UnhandledExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(FunctionStoreFactory.FunctionStoreTask);
}
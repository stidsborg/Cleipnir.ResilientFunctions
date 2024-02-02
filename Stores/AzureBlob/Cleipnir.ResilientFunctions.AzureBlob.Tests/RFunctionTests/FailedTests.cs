using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class FailedTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.FailedTests
{
    [TestMethod]
    public override Task ExceptionThrowingFuncIsNotCompletedByWatchDog()
        => ExceptionThrowingFuncIsNotCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog()
        => ExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(FunctionStoreFactory.Create());

    public override Task UnhandledExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingFuncWithScrapbookIsNotCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExceptionThrowingActionIsNotCompletedByWatchDog()
        => ExceptionThrowingActionIsNotCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PassingInNullParameterResultsInArgumentNullException()
        => PassingInNullParameterResultsInException(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog()
        => ExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task UnhandledExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingActionWithScrapbookIsNotCompletedByWatchDog(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task FuncReturningTaskThrowsSerialization()
        => FuncReturningTaskThrowsSerialization(FunctionStoreFactory.Create());
}
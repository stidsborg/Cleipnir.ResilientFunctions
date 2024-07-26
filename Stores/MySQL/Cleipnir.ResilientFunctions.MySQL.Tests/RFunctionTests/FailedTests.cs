using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.RFunctionTests;

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
    public override Task ExceptionThrowingFuncWithStateIsNotCompletedByWatchDog()
        => ExceptionThrowingFuncWithStateIsNotCompletedByWatchDog(FunctionStoreFactory.Create());

    public override Task UnhandledExceptionThrowingFuncWithStateIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingFuncWithStateIsNotCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExceptionThrowingActionIsNotCompletedByWatchDog()
        => ExceptionThrowingActionIsNotCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PassingInNullParameterResultsInArgumentNullException()
        => PassingInNullParameterResultsInException(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExceptionThrowingActionWithStateIsNotCompletedByWatchDog()
        => ExceptionThrowingActionWithStateIsNotCompletedByWatchDog(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task UnhandledExceptionThrowingActionWithStateIsNotCompletedByWatchDog()
        => UnhandledExceptionThrowingActionWithStateIsNotCompletedByWatchDog(FunctionStoreFactory.Create());
}
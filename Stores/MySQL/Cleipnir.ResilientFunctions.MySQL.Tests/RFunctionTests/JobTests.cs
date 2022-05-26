using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.RFunctionTests;

[TestClass]
public class JobTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.JobTests
{
    [TestMethod]
    public override Task JobCanBeRetried()
        => JobCanBeRetried(Sql.AutoCreateAndInitializeStore()); 

    [TestMethod]
    public override Task JobCanBeStartedMultipleTimesWithoutError()
        => JobCanBeStartedMultipleTimesWithoutError(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CrashedJobIsRetried()
        => CrashedJobIsRetried(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task PostponedJobDoesNotCauseUnhandledException()
        => PostponedJobDoesNotCauseUnhandledException(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task FailedJobDoesCausesUnhandledException()
        => FailedJobDoesCausesUnhandledException(Sql.AutoCreateAndInitializeStore());
}
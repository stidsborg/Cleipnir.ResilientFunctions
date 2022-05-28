using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

[TestClass]
public class JobTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.JobTests
{
    [TestMethod]
    public override Task JobCanBeRetried()
        => JobCanBeRetried(NoSql.AutoCreateAndInitializeStore()); 

    [TestMethod]
    public override Task JobCanBeStartedMultipleTimesWithoutError()
        => JobCanBeStartedMultipleTimesWithoutError(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CrashedJobIsRetried()
        => CrashedJobIsRetried(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task PostponedJobDoesNotCauseUnhandledException()
        => PostponedJobDoesNotCauseUnhandledException(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task FailedJobDoesCausesUnhandledException()
        => FailedJobDoesCausesUnhandledException(NoSql.AutoCreateAndInitializeStore());
}
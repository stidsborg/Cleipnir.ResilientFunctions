using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class AtMostOnceWorkStatusAndResultTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.AtMostOnceWorkStatusAndResultTests
{
    [TestMethod]
    public override Task AtMostOnceWorkIsNotExecutedMultipleTimes()
        => AtMostOnceWorkIsNotExecutedMultipleTimes(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkIsNotExecutedMultipleTimes(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ReferencingGetOnlyPropertyThrowsException()
        => ReferencingGetOnlyPropertyThrowsException(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource()
        => AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource()
        => CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes()
        => AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(Sql.AutoCreateAndInitializeStore());
}
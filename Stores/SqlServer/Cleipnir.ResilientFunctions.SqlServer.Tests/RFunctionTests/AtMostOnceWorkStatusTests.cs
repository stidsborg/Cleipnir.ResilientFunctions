﻿using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class AtMostOnceWorkStatusTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.AtMostOnceWorkStatusTests
{
    [TestMethod]
    public override Task AtMostOnceWorkIsNotExecutedMultipleTimes()
        => AtMostOnceWorkIsNotExecutedMultipleTimes(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource()
        => AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkIsNotExecutedMultipleTimes(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ReferencingGetOnlyPropertyThrowsException()
        => ReferencingGetOnlyPropertyThrowsException(Sql.AutoCreateAndInitializeStore());
}
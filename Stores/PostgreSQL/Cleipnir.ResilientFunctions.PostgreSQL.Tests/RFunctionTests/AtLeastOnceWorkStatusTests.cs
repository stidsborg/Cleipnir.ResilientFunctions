﻿using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class AtLeastOnceWorkStatusTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.AtLeastOnceWorkStatusTests
{
    [TestMethod]
    public override Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted()
        => AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted()
        => AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes()
        => CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes()
        => CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ReferencingGetOnlyPropertyThrowsException()
        => ReferencingGetOnlyPropertyThrowsException(Sql.AutoCreateAndInitializeStore());
}
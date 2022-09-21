﻿using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

[TestClass]
public class MiddlewareExponentialBackoffTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.MiddlewareExponentialBackoffTests
{
    [TestMethod]
    public override Task UnhandledExceptionResultsInPostponedFunc()
        => UnhandledExceptionResultsInPostponedFunc(NoSql.AutoCreateAndInitializeStore());
}
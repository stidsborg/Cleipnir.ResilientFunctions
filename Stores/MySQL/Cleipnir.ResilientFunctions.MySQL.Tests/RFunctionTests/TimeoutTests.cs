﻿using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.RFunctionTests;

[TestClass]
public class TimeoutTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.TimeoutTests
{
    [TestMethod]
    public override Task ExpiredTimeoutIsAddedToEventSource()
        => ExpiredTimeoutIsAddedToEventSource(Sql.AutoCreateAndInitializeStore());
}
﻿using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class TimeoutTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.TimeoutTests
{
    [TestMethod]
    public override Task ExpiredTimeoutIsAddedToMessages()
        => ExpiredTimeoutIsAddedToMessages(FunctionStoreFactory.Create());
}
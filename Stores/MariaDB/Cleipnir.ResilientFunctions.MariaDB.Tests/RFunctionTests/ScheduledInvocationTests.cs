﻿using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests.RFunctionTests;

[TestClass]
public class ScheduledInvocationTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.ScheduledInvocationTests
{
    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterFuncStateHasBeenPersisted(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterFuncWithStateHasBeenPersisted(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionWithStateHasBeenPersisted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted()
        => ScheduledFunctionIsInvokedAfterActionStateHasBeenPersisted(FunctionStoreFactory.Create());
}
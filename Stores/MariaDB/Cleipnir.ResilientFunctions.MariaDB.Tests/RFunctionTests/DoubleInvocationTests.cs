﻿using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests.RFunctionTests;

[TestClass]
public class DoubleInvocationTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.DoubleInvocationTests
{
    [TestMethod]
    public override Task SecondInvocationWaitsForAndReturnsSuccessfulResult()
        => SecondInvocationWaitsForAndReturnsSuccessfulResult(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task SecondInvocationFailsOnSuspendedFlow()
        => SecondInvocationFailsOnSuspendedFlow(FunctionStoreFactory.Create());
        
    [TestMethod]
    public override Task SecondInvocationFailsOnPostponedFlow()
        => SecondInvocationFailsOnPostponedFlow(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task SecondInvocationFailsOnFailedFlow()
        => SecondInvocationFailsOnFailedFlow(FunctionStoreFactory.Create());
}
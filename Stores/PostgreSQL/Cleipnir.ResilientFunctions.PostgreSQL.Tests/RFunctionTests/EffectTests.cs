using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class EffectTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.EffectTests
{
    [TestMethod]
    public override Task SunshineActionTest()
        => SunshineActionTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineAsyncActionTest()
        => SunshineAsyncActionTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineFuncTest()
        => SunshineFuncTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineAsyncFuncTest()
        => SunshineAsyncFuncTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExceptionThrowingActionTest()
        => ExceptionThrowingActionTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task TaskWhenAnyFuncTest()
        => TaskWhenAnyFuncTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task TaskWhenAllFuncTest()
        => TaskWhenAllFuncTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ClearEffectsTest()
        => ClearEffectsTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExistingEffectsFuncIsOnlyInvokedAfterGettingValue()
        => ExistingEffectsFuncIsOnlyInvokedAfterGettingValue(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task SubEffectHasImplicitContext()
        => SubEffectHasImplicitContext(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SubEffectHasExplicitContext()
        => SubEffectHasExplicitContext(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task EffectsCrudTest()
        => EffectsCrudTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExceptionThrownInsideEffectBecomesFatalWorkflowException()
        => ExceptionThrownInsideEffectBecomesFatalWorkflowException(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExceptionThrownInsideEffectStaysFatalWorkflowException()
        => ExceptionThrownInsideEffectStaysFatalWorkflowException(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task EffectCanReturnOption()
        => EffectCanReturnOption(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task DelayedFlushIsReflectedInUnderlyingStoreForSet()
        => DelayedFlushIsReflectedInUnderlyingStoreForSet(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task CaptureUsingAtLeastOnceWithoutFlushResiliencyDelaysFlush()
        => CaptureUsingAtLeastOnceWithoutFlushResiliencyDelaysFlush(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task CaptureUsingAtLeastOnceWithoutFlushResiliencyDelaysFlushInFlow()
        => CaptureUsingAtLeastOnceWithoutFlushResiliencyDelaysFlushInFlow(FunctionStoreFactory.Create());
}
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
    public override Task FuncWithNullValueTest()
        => FuncWithNullValueTest(FunctionStoreFactory.Create());

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
    public override Task ExistingEffectsFuncIsOnlyInvokedAfterGettingValue()
        => ExistingEffectsFuncIsOnlyInvokedAfterGettingValue(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task SubEffectHasImplicitContext()
        => SubEffectHasImplicitContext(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SubEffectHasExplicitContext()
        => SubEffectHasExplicitContext(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EffectsHasCorrectlyOrderedIds()
        => EffectsHasCorrectlyOrderedIds(FunctionStoreFactory.Create());

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
    public override Task DelayedFlushIsReflectedInUnderlyingStoreForSet()
        => DelayedFlushIsReflectedInUnderlyingStoreForSet(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task CaptureUsingAtLeastOnceWithoutFlushResiliencyDelaysFlush()
        => CaptureUsingAtLeastOnceWithoutFlushResiliencyDelaysFlush(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task CaptureUsingAtLeastOnceWithoutFlushResiliencyDelaysFlushInFlow()
        => CaptureUsingAtLeastOnceWithoutFlushResiliencyDelaysFlushInFlow(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task UpsertingExistingEffectDoesNotAffectOtherExistingEffects()
        => UpsertingExistingEffectDoesNotAffectOtherExistingEffects(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CaptureEffectWithRetryPolicy()
        => CaptureEffectWithRetryPolicy(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task CaptureEffectWithRetryPolicyWithResult()
        => CaptureEffectWithRetryPolicyWithResult(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task CaptureEffectWithRetryPolicyWithoutSuspension()
        => CaptureEffectWithRetryPolicyWithoutSuspension(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExceptionPredicateIsUsedForRetryPolicy()
        => ExceptionPredicateIsUsedForRetryPolicy(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EffectLoopingWorks()
        => EffectLoopingWorks(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ChildEffectsAreClearedWhenParentEffectWithResultCompletes()
        => ChildEffectsAreClearedWhenParentEffectWithResultCompletes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ChildEffectsAreClearedWhenParentEffectReturningValueCompletes()
        => ChildEffectsAreClearedWhenParentEffectReturningValueCompletes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task AggregateEachBasicAggregationWorks()
        => AggregateEachBasicAggregationWorks(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task AggregateEachResumesMidAggregation()
        => AggregateEachResumesMidAggregation(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task AggregateEachWithComplexAccumulator()
        => AggregateEachWithComplexAccumulator(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task AggregateEachCleansUpIntermediateEffects()
        => AggregateEachCleansUpIntermediateEffects(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task AggregateEachWithSingleElement()
        => AggregateEachWithSingleElement(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task GetChildrenReturnsAllChildEffectValues()
        => GetChildrenReturnsAllChildEffectValues(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task GetChildrenReturnsEmptyListWhenNoChildren()
        => GetChildrenReturnsEmptyListWhenNoChildren(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task GetChildrenReturnsAllDescendants()
        => GetChildrenReturnsAllDescendants(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task RunParallelleTest()
        => RunParallelleTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task UtcNowEffectSunshineTest()
        => UtcNowEffectSunshineTest(FunctionStoreFactory.Create());
}
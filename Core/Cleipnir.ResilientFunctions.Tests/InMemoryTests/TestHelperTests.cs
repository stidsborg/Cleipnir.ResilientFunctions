using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class TestHelperTests
{
    [TestMethod]
    public async Task UseAnonymousFunctionTestHelper()
    {
        using var testHelper = TestHelper.Create().ForAnonymous();
        
        async Task<string> InnerMethod(string param, Context context)
        {
            var es = await context.EventSource;
            // ReSharper disable once AccessToDisposedClosure
            context.FunctionId.ShouldBe(testHelper.FunctionId);
            return await es.OfType<string>().Next();
        }
        
        var innerMethodTask = InnerMethod(param: "", testHelper.Context);
        innerMethodTask.IsCompleted.ShouldBeFalse();

        await testHelper.EventSourceWriter.AppendEvent("hello world");
        var returned = await innerMethodTask;
        returned.ShouldBe("hello world");
    }
    
    [TestMethod]
    public async Task UseProvidedFunctionIdTestHelper()
    {
        var functionId = new FunctionId("someFunctionId", "someFunctionInstance");
        using var testHelper = TestHelper.Create().For(functionId);
        
        async Task<string> InnerMethod(string param, Context context)
        {
            var es = await context.EventSource;
            // ReSharper disable once AccessToDisposedClosure
            context.FunctionId.ShouldBe(testHelper.FunctionId);
            return await es.OfType<string>().Next();
        }
        
        var innerMethodTask = InnerMethod(param: "", testHelper.Context);
        innerMethodTask.IsCompleted.ShouldBeFalse();

        await testHelper.EventSourceWriter.AppendEvent("hello world");
        var returned = await innerMethodTask;
        returned.ShouldBe("hello world");
    }
}
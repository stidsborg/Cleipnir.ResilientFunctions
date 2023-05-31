using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class AtMostOnceWorkStatusTests
{
    [TestMethod]
    public async Task AtMostOnceWorkIsNotExecutedMultipleTimes()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        
        var rAction = rFunctions.RegisterAction(
            "",
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtMostOnce(
                        workStatus: s => s.WorkStatus,
                        work: () =>
                        {
                            counter.Increment();
                            throw new PostponeInvocationException(1);
                        }
                    );
            }).Invoke;

        _ = rAction.Invoke("", "hello");

        await BusyWait.Until(() =>
            store.GetFunction(new FunctionId("", ""))
                .SelectAsync(sf => sf?.Status == Status.Failed)
        );
        
        counter.Current.ShouldBe(1);
    }
    
    [TestMethod]
    public async Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        
        var rAction = rFunctions.RegisterAction(
            "",
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtMostOnce(
                        workId: "someId",
                        work: () =>
                        {
                            counter.Increment();
                            throw new PostponeInvocationException(1);
                        }
                    );
            }).Invoke;

        _ = rAction.Invoke("", "hello");

        await BusyWait.Until(() =>
            store.GetFunction(new FunctionId("", ""))
                .SelectAsync(sf => sf?.Status == Status.Failed)
        );
        
        counter.Current.ShouldBe(1);
    }
    
    [TestMethod]
    public async Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        
        var rAction = rFunctions.RegisterAction(
            "",
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtMostOnce(
                        workStatus: s => s.WorkStatus,
                        work: () => { counter.Increment(); return Task.CompletedTask; });
            });

        await rAction.Invoke("", "hello");
        await rAction.ControlPanels.For("").Result!.ReInvoke();

        counter.Current.ShouldBe(1);
    }
    
    [TestMethod]
    public async Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        
        var rAction = rFunctions.RegisterAction(
            "",
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtMostOnce(
                        workId: "someId",
                        work: () => { counter.Increment(); return Task.CompletedTask; });
            });

        await rAction.Invoke("", "hello");
        await rAction.ControlPanels.For("").Result!.ReInvoke();

        counter.Current.ShouldBe(1);
    }
    
    [TestMethod]
    public async Task ReferencingGetOnlyPropertyThrowsException()
    {
        var scrapbook = new ScrapbookGetterOnly();
        await Should.ThrowAsync<ArgumentException>(() => 
            scrapbook.DoAtMostOnce(
                workStatus: s => s.WorkStatus,
                work: () => Task.CompletedTask
            )
        );
    }

    private class Scrapbook : RScrapbook
    {
        public WorkStatus WorkStatus { get; set; }
    }
    
    private class ScrapbookGetterOnly : RScrapbook
    {
        public WorkStatus WorkStatus { get; } = WorkStatus.NotStarted;
    }
}
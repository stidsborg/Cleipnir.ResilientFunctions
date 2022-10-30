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
public class AtLeastOnceTests
{
    [TestMethod]
    public async Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted()
    {
        var store = new InMemoryFunctionStore();
        var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        
        var rAction = rFunctions.RegisterAction(
            "",
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtLeastOnce(
                        workStatus: s => s.WorkStatus,
                        work: () =>
                        {
                            counter.Increment();
                            if (counter.Current == 1)
                                throw new PostponeInvocationException(1);
                            return Task.CompletedTask;
                        }
                    );
            });

        _ = rAction.Invoke("", "hello");

        await BusyWait.Until(() =>
            store.GetFunction(new FunctionId("", ""))
                .SelectAsync(sf => sf?.Status == Status.Succeeded)
        );

        counter.Current.ShouldBe(2);
    }
    
    [TestMethod]
    public async Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted()
    {
        var store = new InMemoryFunctionStore();
        var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        
        var rAction = rFunctions.RegisterAction(
            "",
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtLeastOnce(
                        workId: "someId",
                        work: () =>
                        {
                            counter.Increment();
                            if (counter.Current == 1)
                                throw new PostponeInvocationException(1);
                            return Task.CompletedTask;
                        }
                    );
            });

        _ = rAction.Invoke("", "hello");

        await BusyWait.Until(() =>
            store.GetFunction(new FunctionId("", ""))
                .SelectAsync(sf => sf?.Status == Status.Succeeded)
        );

        counter.Current.ShouldBe(2);
    }
    
    [TestMethod]
    public async Task CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes()
    {
        var store = new InMemoryFunctionStore();
        var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        
        var rAction = rFunctions.RegisterAction(
            "",
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtLeastOnce(
                        workStatus: s => s.WorkStatus,
                        work: () => { counter.Increment(); return Task.CompletedTask; });
            });

        await rAction.Invoke("", "hello");
        await rAction.ControlPanel.For("").Result!.ReInvoke();

        counter.Current.ShouldBe(1);
    }
    
    [TestMethod]
    public async Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes()
    {
        var store = new InMemoryFunctionStore();
        var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        
        var rAction = rFunctions.RegisterAction(
            "",
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtLeastOnce(
                        workId: "someId",
                        work: () => { counter.Increment(); return Task.CompletedTask; });
            });

        await rAction.Invoke("", "hello");
        await rAction.ControlPanel.For("").Result!.ReInvoke();

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
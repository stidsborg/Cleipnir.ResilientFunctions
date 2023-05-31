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
public class AtLeastOnceWorkStatusAndResultTests
{
    [TestMethod]
    public async Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
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
                            
                            return 1.ToTask();
                        }
                    );
            });

        _ = rAction.Invoke("", "hello");
        
        await BusyWait.Until(() =>
            store.GetFunction(new FunctionId("", ""))
                .SelectAsync(sf => sf?.Status == Status.Succeeded)
        );

        var controlPanel = await rAction.ControlPanels.For("");
        controlPanel!.Scrapbook.WorkStatus.Result.ShouldBe(1);
        counter.Current.ShouldBe(2);
    }
    
    [TestMethod]
    public async Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
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
                            
                            return "hello world".ToTask();
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
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        
        var rAction = rFunctions.RegisterAction(
            "",
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtLeastOnce(
                        workStatus: s => s.WorkStatus,
                        work: () => { counter.Increment(); return 1.ToTask(); });
            });

        await rAction.Invoke("", "hello");
        await rAction.ControlPanels.For("").Result!.ReInvoke();

        counter.Current.ShouldBe(1);
    }
    
    [TestMethod]
    public async Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        
        var rAction = rFunctions.RegisterAction(
            "",
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtLeastOnce(
                        workId: "someId",
                        work: () => { counter.Increment(); return "hello world".ToTask(); }
                    );
            });

        await rAction.Invoke("", "hello");
        var controlPanel = await rAction.ControlPanels.For("");
        controlPanel.ShouldNotBeNull();
        
        await controlPanel.ReInvoke();
        await controlPanel.Refresh();

        var value = controlPanel.Scrapbook.StateDictionary["someId"];
        var splitValue = value.Split(",");
        splitValue[0].ShouldBe(WorkStatus.Completed.ToString());
        splitValue[1].ShouldBe("hello world");
        counter.Current.ShouldBe(1);
    }

    [TestMethod]
    public async Task ReferencingGetOnlyPropertyThrowsException()
    {
        var scrapbook = new ScrapbookGetterOnly();
        await Should.ThrowAsync<ArgumentException>(() => 
            scrapbook.DoAtMostOnce(
                workStatus: s => s.WorkStatus,
                work: () => 1.ToTask()
            )
        );
    }

    private class Scrapbook : RScrapbook
    {
        public WorkStatusAndResult<int> WorkStatus { get; set; }
    }
    
    private class ScrapbookGetterOnly : RScrapbook
    {
        public WorkStatusAndResult<int> WorkStatus { get; } = new();
    }
}
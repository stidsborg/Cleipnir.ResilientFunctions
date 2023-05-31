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
public class AtMostOnceWorkStatusAndResultTests
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
                            if (counter.Current != 0)
                                throw new PostponeInvocationException(1);

                            return "hello world".ToTask();
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
                        work: () => { counter.Increment(); return 1.ToTask(); }
                    );
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
                        work: () => { counter.Increment(); return "hello world".ToTask(); }
                    );
            });

        await rAction.Invoke("", "hello");
        var controlPanel = await rAction.ControlPanels.For("");
        controlPanel.ShouldNotBeNull();

        await controlPanel.ReInvoke();

        counter.Current.ShouldBe(1);
        await controlPanel.Refresh();

        var value = controlPanel.Scrapbook.StateDictionary["someId"];
        var splitValue = value.Split(",");
        splitValue[0].ShouldBe(WorkStatus.Completed.ToString());
        splitValue[1].ShouldBe("hello world");
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
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

[TestClass]
public class ReactiveIntegrationTests
{
    [TestMethod]
    public async Task SyncingStopsAfterReactiveChainCompletion()
    {
        var counter = new SyncedCounter();
        
        var source = new TestSource(
            NoOpRegisteredTimeouts.Instance,
            syncStore: _ =>
            {
                counter.Increment();
                return Task.CompletedTask;
            });

        var listTask = source.Take(1).ToList();
        source.SignalNext(1);
        
        await listTask;
        var beforeDelayCounter = counter.Current;
        await Task.Delay(250);
        counter.Current.ShouldBe(beforeDelayCounter);
    }
}
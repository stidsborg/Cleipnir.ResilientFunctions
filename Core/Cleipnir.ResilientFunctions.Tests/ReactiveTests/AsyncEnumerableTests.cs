using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Origin;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

[TestClass]
public class AsyncEnumerableTests
{
    [TestMethod]
    public async Task AsyncEnumerableSunshineTest()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var emits = new SyncedList<string>();
        source.SignalNext("hello", new InterruptCount(1));

        async Task AwaitForeach()
        {
            await foreach (var item in source.OfType<string>())
                emits.Add(item);
        }

        var task = AwaitForeach();
        await BusyWait.UntilAsync(() => emits.Count == 1);
        emits.Count.ShouldBe(1);
        emits[0].ShouldBe("hello");
        task.IsCompleted.ShouldBeFalse();

        source.SignalNext("world", new InterruptCount(2));
        await BusyWait.UntilAsync(() => emits.Count == 2);
        emits.Count.ShouldBe(2);
        emits[1].ShouldBe("world");
        task.IsCompleted.ShouldBeFalse();
        
        source.SignalCompletion();
        await BusyWait.UntilAsync(() => task.IsCompletedSuccessfully);
        emits.Count.ShouldBe(2);

        await task;
    }
    
    [TestMethod]
    public async Task AsyncEnumerableThrownExceptionTest()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var emits = new SyncedList<string>();
        source.SignalNext("hello", new InterruptCount(1));

        async Task AwaitForeach()
        {
            await foreach (var item in source.OfType<string>())
                emits.Add(item);
        }

        var task = AwaitForeach();
        await BusyWait.UntilAsync(() => emits.Count == 1);
        emits.Count.ShouldBe(1);
        emits[0].ShouldBe("hello");
        task.IsCompleted.ShouldBeFalse();

        source.SignalError(new TimeoutException());
        await BusyWait.UntilAsync(() => task.IsFaulted);
        await Should.ThrowAsync<TimeoutException>(task);
    }
}
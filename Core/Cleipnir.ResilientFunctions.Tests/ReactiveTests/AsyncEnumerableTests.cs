﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
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
        var source = new TestSource();
        var emits = new SyncedList<string>();
        source.SignalNext("hello");

        async Task AwaitForeach()
        {
            await foreach (var item in source.OfType<string>())
                emits.Add(item);
        }

        var task = AwaitForeach();
        await BusyWait.Until(() => emits.Count == 1);
        emits.Count.ShouldBe(1);
        emits[0].ShouldBe("hello");
        task.IsCompleted.ShouldBeFalse();

        source.SignalNext("world");
        await BusyWait.Until(() => emits.Count == 2);
        emits.Count.ShouldBe(2);
        emits[1].ShouldBe("world");
        task.IsCompleted.ShouldBeFalse();
        
        source.SignalCompletion();
        await BusyWait.Until(() => task.IsCompletedSuccessfully);
        emits.Count.ShouldBe(2);

        await task;
    }
    
    [TestMethod]
    public async Task AsyncEnumerableThrownExceptionTest()
    {
        var source = new TestSource();
        var emits = new SyncedList<string>();
        source.SignalNext("hello");

        async Task AwaitForeach()
        {
            await foreach (var item in source.OfType<string>())
                emits.Add(item);
        }

        var task = AwaitForeach();
        await BusyWait.Until(() => emits.Count == 1);
        emits.Count.ShouldBe(1);
        emits[0].ShouldBe("hello");
        task.IsCompleted.ShouldBeFalse();

        source.SignalError(new TimeoutException());
        await BusyWait.Until(() => task.IsFaulted);
        await Should.ThrowAsync<TimeoutException>(task);
    }
    
    [TestMethod]
    public async Task AsyncEnumerableSuspendsAfterMaxWait()
    {
        var source = new TestSource(maxWait: TimeSpan.FromSeconds(1));
        var emits = new SyncedList<string>();
        var asyncEnumerableTask = Should.ThrowAsync<SuspendInvocationException>(async () =>
        {
            await foreach (var item in source.OfType<string>())
                emits.Add(item);
        });
        await Task.Delay(500);
        source.SignalNext("hello");

        await asyncEnumerableTask;
        emits.Count.ShouldBe(1);
        emits[0].ShouldBe("hello");
    }
    
    [TestMethod]
    public async Task AsyncEnumerableDoesSuspendWhenCompletionIsBeforeMaxWait()
    {
        var source = new TestSource(maxWait: TimeSpan.FromSeconds(1));
        var emits = new SyncedList<string>();
        source.SignalNext("hello");
        await foreach (var item in source.OfType<string>().Take(1))
            emits.Add(item);
        
        emits.Count.ShouldBe(1);
        emits[0].ShouldBe("hello");
    }
    
    [TestMethod]
    public async Task AsyncEnumerableSuspendsWhenMaxWaitIsZeroAndNoMessagesArePending()
    {
        var source = new TestSource(maxWait: TimeSpan.Zero);
        await Should.ThrowAsync<SuspendInvocationException>(async () =>
        {
            await foreach (var item in source.OfType<string>())
            {
                //nothing
            }
        });
    }
}
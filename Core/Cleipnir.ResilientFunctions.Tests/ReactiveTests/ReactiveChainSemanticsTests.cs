using System;
using System.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Origin;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

[TestClass]
public class ReactiveChainSemanticsTests
{
    [TestMethod]
    public void StreamCanBeReplayedToCertainEventCountWhenCompletedEarlySuccessfully()
    {
        var source = new TestSource();
        source.SignalNext("hello");
        source.SignalNext("world");

        var completed = false;
        var failed = false;
        var latest = "";
        var subscription = source.OfType<string>().Take(1).Subscribe(
            onNext: s => latest = s,
            onCompletion: () => completed = true,
            onError: _ => failed = true
        );

        completed.ShouldBeFalse();
        failed.ShouldBeFalse();
        latest.ShouldBe("");

        subscription.PushMessages();
            
        completed.ShouldBeTrue();
        failed.ShouldBeFalse();
        latest.ShouldBe("hello");
    }
        
    [TestMethod]
    public void StreamCanBeReplayedToCertainEventCountWhenFailedEarlySuccessfully()
    {
        var source = new TestSource();
        source.SignalNext("hello");
        source.SignalNext("world");

        var completed = false;
        var failed = false;
        var latest = "";
        var subscription = source
            .OfType<string>()
            .Select<string, string>(_ => throw new Exception("oh no"))
            .Subscribe(
                onNext: s => latest = s,
                onCompletion: () => completed = true,
                onError: _ => failed = true
            );

        completed.ShouldBeFalse();
        failed.ShouldBeFalse();
        latest.ShouldBe("");

        subscription.PushMessages();
            
        completed.ShouldBeFalse();
        failed.ShouldBeTrue();
        latest.ShouldBe("");
    }
    
    [TestMethod]
    public void ExistingPropertyContainsPreviouslyEmittedEvents()
    {
        var source = new TestSource();
        source.SignalNext("hello");
        var existing = source.Existing.ToList();
        existing.Count.ShouldBe(1);
        existing[0].ShouldBe("hello");

        source.SignalNext("world"); 
        
        existing = source.Existing.ToList();
        existing.Count.ShouldBe(2);
        existing[0].ShouldBe("hello");
        existing[1].ShouldBe("world");
    }

    [TestMethod]
    public void StreamUnsubscribesToFutureEventsAfterDeliverExistingInvocation()
    {
        var source = new TestSource();
        var emitted = 0;

        source.SignalNext("hello");
        
        var subscription = source
            .Subscribe(
                onNext: _ => emitted++,
                onError: _ => { },
                onCompletion: () => { }
            );

        subscription.PushMessages();
        
        emitted.ShouldBe(1);

        source.SignalNext("world");
        
        emitted.ShouldBe(1);
    }
}
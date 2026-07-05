using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class CreateNextChildTests
{
    private static Effect CreateEffect(IReadOnlyList<StoredEffect> existingEffects)
    {
        var storedId = TestStoredId.Create();
        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            existingEffects,
            new InMemoryFunctionStore().EffectsStore,
            DefaultSerializer.Instance,
            storageSession: null,
            clearChildren: true
        );

        return new Effect(
            effectResults,
            utcNow: () => DateTime.UtcNow,
            new FlowTimeouts(),
            new FlowExecutionState(storedId, subflows: 1, waitingSubflows: 0, new FlowTimeouts(), completed: ForeverTask.Instance)
        );
    }

    [TestMethod]
    public void CreateNextChildAppendsSequentiallyFromZero()
    {
        var effect = CreateEffect(existingEffects: new List<StoredEffect>());
        var parent = new EffectId([1]);

        var first = effect.FlushlessCreateNextChild(parent, content: "a");
        var second = effect.FlushlessCreateNextChild(parent, content: "b");
        var third = effect.FlushlessCreateNextChild(parent, content: "c");

        first.ShouldBe(parent.CreateChild(0));
        second.ShouldBe(parent.CreateChild(1));
        third.ShouldBe(parent.CreateChild(2));

        // Flushless writes are visible in-memory immediately (no Flush required).
        effect.Get<string>(first).ShouldBe("a");
        effect.Get<string>(second).ShouldBe("b");
        effect.Get<string>(third).ShouldBe("c");
    }

    [TestMethod]
    public void CreateNextChildContinuesFromExistingChildren()
    {
        var parent = new EffectId([7]);
        var existing = new List<StoredEffect>
        {
            StoredEffect.CreateCompleted(parent.CreateChild(0), alias: null),
            StoredEffect.CreateCompleted(parent.CreateChild(1), alias: null),
        };
        var effect = CreateEffect(existing);

        var next = effect.FlushlessCreateNextChild(parent, content: "x");

        next.ShouldBe(parent.CreateChild(2));
    }

    [TestMethod]
    public async Task CreateNextChildKeepsGapAndUsesHighestPlusOne()
    {
        var effect = CreateEffect(existingEffects: new List<StoredEffect>());
        var parent = new EffectId([1]);

        effect.FlushlessCreateNextChild(parent, content: "a"); // [1,0]
        var middle = effect.FlushlessCreateNextChild(parent, content: "b"); // [1,1]
        effect.FlushlessCreateNextChild(parent, content: "c"); // [1,2]

        await effect.Clear(middle, flush: true); // remove [1,1], leaving {0,2}

        var next = effect.FlushlessCreateNextChild(parent, content: "d");

        // Highest existing index (2) + 1 -> gap at index 1 is kept.
        next.ShouldBe(parent.CreateChild(3));
    }

    [TestMethod]
    public void CreateNextChildIsScopedToItsParent()
    {
        var effect = CreateEffect(existingEffects: new List<StoredEffect>());
        var parentA = new EffectId([1]);
        var parentB = new EffectId([2]);

        var a0 = effect.FlushlessCreateNextChild(parentA, content: "a0");
        var a1 = effect.FlushlessCreateNextChild(parentA, content: "a1");
        var b0 = effect.FlushlessCreateNextChild(parentB, content: "b0");

        a0.ShouldBe(parentA.CreateChild(0));
        a1.ShouldBe(parentA.CreateChild(1));
        b0.ShouldBe(parentB.CreateChild(0));
    }
}

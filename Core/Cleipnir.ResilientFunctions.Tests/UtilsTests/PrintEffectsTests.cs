using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.UtilsTests;

[TestClass]
public class PrintEffectsTests
{
    private static string StripAnsiColors(string input)
    {
        return Regex.Replace(input, @"\x1b\[[0-9;]*m", string.Empty);
    }
    [TestMethod]
    public void PrintSingleCompletedEffect()
    {
        var storedId = TestStoredId.Create();
        var existingEffects = new List<StoredEffect>
        {
            new StoredEffect(
                new EffectId([1]),
                WorkStatus.Completed,
                Result: null,
                StoredException: null,
                Alias: null
            )
        };

        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            existingEffects,
            new InMemoryFunctionStore().EffectsStore,
            DefaultSerializer.Instance,
            storageSession: null,
            clearChildren: true
        );

        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowTimeouts());
        var output = effect.ExecutionTree();

        var expected = "└─ ✓ [1]\n";
        StripAnsiColors(output).ShouldBe(expected);
    }

    [TestMethod]
    public void PrintEffectWithAlias()
    {
        var storedId = TestStoredId.Create();
        var existingEffects = new List<StoredEffect>
        {
            new StoredEffect(
                new EffectId([1]),
                WorkStatus.Completed,
                Result: null,
                StoredException: null,
                Alias: "my-effect"
            )
        };

        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            existingEffects,
            new InMemoryFunctionStore().EffectsStore,
            DefaultSerializer.Instance,
            storageSession: null,
            clearChildren: true
        );

        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowTimeouts());
        var output = effect.ExecutionTree();

        var expected = "└─ ✓ [1] my-effect\n";
        StripAnsiColors(output).ShouldBe(expected);
    }

    [TestMethod]
    public void PrintFailedEffect()
    {
        var storedId = TestStoredId.Create();
        var existingEffects = new List<StoredEffect>
        {
            new StoredEffect(
                new EffectId([1]),
                WorkStatus.Failed,
                Result: null,
                StoredException: new StoredException(
                    "Something went wrong",
                    "Stack trace here",
                    "System.InvalidOperationException"
                ),
                Alias: "failed-operation"
            )
        };

        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            existingEffects,
            new InMemoryFunctionStore().EffectsStore,
            DefaultSerializer.Instance,
            storageSession: null,
            clearChildren: true
        );

        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowTimeouts());
        var output = effect.ExecutionTree();

        var expected = "└─ ✗ [1] failed-operation (System.InvalidOperationException)\n";
        StripAnsiColors(output).ShouldBe(expected);
    }

    [TestMethod]
    public void PrintStartedEffect()
    {
        var storedId = TestStoredId.Create();
        var existingEffects = new List<StoredEffect>
        {
            new StoredEffect(
                new EffectId([1]),
                WorkStatus.Started,
                Result: null,
                StoredException: null,
                Alias: "in-progress"
            )
        };

        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            existingEffects,
            new InMemoryFunctionStore().EffectsStore,
            DefaultSerializer.Instance,
            storageSession: null,
            clearChildren: true
        );

        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowTimeouts());
        var output = effect.ExecutionTree();

        var expected = "└─ ⋯ [1] in-progress\n";
        StripAnsiColors(output).ShouldBe(expected);
    }

    [TestMethod]
    public void PrintEffectHierarchy()
    {
        var storedId = TestStoredId.Create();
        var existingEffects = new List<StoredEffect>
        {
            new StoredEffect(
                new EffectId([1]),
                WorkStatus.Completed,
                Result: null,
                StoredException: null,
                Alias: "parent"
            ),
            new StoredEffect(
                new EffectId([1, 1]),
                WorkStatus.Completed,
                Result: null,
                StoredException: null,
                Alias: "child-1"
            ),
            new StoredEffect(
                new EffectId([1, 2]),
                WorkStatus.Completed,
                Result: null,
                StoredException: null,
                Alias: "child-2"
            )
        };

        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            existingEffects,
            new InMemoryFunctionStore().EffectsStore,
            DefaultSerializer.Instance,
            storageSession: null,
            clearChildren: true
        );

        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowTimeouts());
        var output = effect.ExecutionTree();

        var expected =
            "└─ ✓ [1] parent\n" +
            "   ├─ ✓ [1] child-1\n" +
            "   └─ ✓ [2] child-2\n";
        StripAnsiColors(output).ShouldBe(expected);
    }

    [TestMethod]
    public void PrintDeepEffectHierarchy()
    {
        var storedId = TestStoredId.Create();
        var existingEffects = new List<StoredEffect>
        {
            new StoredEffect(
                new EffectId([1]),
                WorkStatus.Completed,
                Result: null,
                StoredException: null,
                Alias: "root"
            ),
            new StoredEffect(
                new EffectId([1, 1]),
                WorkStatus.Completed,
                Result: null,
                StoredException: null,
                Alias: "level-1"
            ),
            new StoredEffect(
                new EffectId([1, 1, 1]),
                WorkStatus.Completed,
                Result: null,
                StoredException: null,
                Alias: "level-2"
            ),
            new StoredEffect(
                new EffectId([1, 1, 1, 1]),
                WorkStatus.Failed,
                Result: null,
                StoredException: new StoredException("Deep error", null, "System.Exception"),
                Alias: "level-3-failed"
            )
        };

        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            existingEffects,
            new InMemoryFunctionStore().EffectsStore,
            DefaultSerializer.Instance,
            storageSession: null,
            clearChildren: true
        );

        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowTimeouts());
        var output = effect.ExecutionTree();

        var expected =
            "└─ ✓ [1] root\n" +
            "   └─ ✓ [1] level-1\n" +
            "      └─ ✓ [1] level-2\n" +
            "         └─ ✗ [1] level-3-failed (System.Exception)\n";
        StripAnsiColors(output).ShouldBe(expected);
    }

    [TestMethod]
    public void PrintMultipleRootEffects()
    {
        var storedId = TestStoredId.Create();
        var existingEffects = new List<StoredEffect>
        {
            new StoredEffect(
                new EffectId([1]),
                WorkStatus.Completed,
                Result: null,
                StoredException: null,
                Alias: "first-root"
            ),
            new StoredEffect(
                new EffectId([2]),
                WorkStatus.Started,
                Result: null,
                StoredException: null,
                Alias: "second-root"
            ),
            new StoredEffect(
                new EffectId([3]),
                WorkStatus.Failed,
                Result: null,
                StoredException: new StoredException("Error", null, "System.Exception"),
                Alias: "third-root"
            )
        };

        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            existingEffects,
            new InMemoryFunctionStore().EffectsStore,
            DefaultSerializer.Instance,
            storageSession: null,
            clearChildren: true
        );

        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowTimeouts());
        var output = effect.ExecutionTree();

        var expected =
            "└─ ✓ [1] first-root\n" +
            "└─ ⋯ [2] second-root\n" +
            "└─ ✗ [3] third-root (System.Exception)\n";
        StripAnsiColors(output).ShouldBe(expected);
    }

    [TestMethod]
    public void PrintComplexEffectTree()
    {
        var storedId = TestStoredId.Create();
        var existingEffects = new List<StoredEffect>
        {
            new StoredEffect(new EffectId([1]), WorkStatus.Completed, null, null, "root-1"),
            new StoredEffect(new EffectId([1, 1]), WorkStatus.Completed, null, null, "root-1-child-1"),
            new StoredEffect(new EffectId([1, 2]), WorkStatus.Completed, null, null, "root-1-child-2"),
            new StoredEffect(new EffectId([1, 2, 1]), WorkStatus.Failed, null,
                new StoredException("Error", null, "TestException"), "root-1-child-2-grandchild"),
            new StoredEffect(new EffectId([2]), WorkStatus.Started, null, null, "root-2"),
            new StoredEffect(new EffectId([2, 1]), WorkStatus.Completed, null, null, "root-2-child-1")
        };

        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            existingEffects,
            new InMemoryFunctionStore().EffectsStore,
            DefaultSerializer.Instance,
            storageSession: null,
            clearChildren: true
        );

        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowTimeouts());
        var output = effect.ExecutionTree();

        var expected =
            "└─ ✓ [1] root-1\n" +
            "   ├─ ✓ [1] root-1-child-1\n" +
            "   └─ ✓ [2] root-1-child-2\n" +
            "      └─ ✗ [1] root-1-child-2-grandchild (TestException)\n" +
            "└─ ⋯ [2] root-2\n" +
            "   └─ ✓ [1] root-2-child-1\n";
        StripAnsiColors(output).ShouldBe(expected);
    }

    [TestMethod]
    public void PrintEffectTreeWithMissingIntermediateEffect()
    {
        // This test verifies that missing intermediate effects are automatically added with Started status
        var storedId = TestStoredId.Create();
        var existingEffects = new List<StoredEffect>
        {
            new StoredEffect(new EffectId([1]), WorkStatus.Completed, null, null, "root"),
            // Missing [1, 2] - this should be automatically added
            new StoredEffect(new EffectId([1, 2, 1]), WorkStatus.Completed, null, null, "grandchild")
        };

        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            existingEffects,
            new InMemoryFunctionStore().EffectsStore,
            DefaultSerializer.Instance,
            storageSession: null,
            clearChildren: false
        );

        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowTimeouts());
        var output = effect.ExecutionTree();

        var expected =
            "└─ ✓ [1] root\n" +
            "   └─ ⋯ [2]\n" +  // Missing intermediate effect added with Started status (⋯) and no alias
            "      └─ ✓ [1] grandchild\n";
        StripAnsiColors(output).ShouldBe(expected);
    }

    [TestMethod]
    public void PrintEffectTreeWithMultipleMissingAncestors()
    {
        // Verifies that multiple missing ancestors are added when a deep descendant exists
        var storedId = TestStoredId.Create();
        var existingEffects = new List<StoredEffect>
        {
            new StoredEffect(new EffectId([1]), WorkStatus.Completed, null, null, "root"),
            // Missing [1, 2] and [1, 2, 3] - both should be automatically added
            new StoredEffect(new EffectId([1, 2, 3, 4]), WorkStatus.Failed, null,
                new StoredException("Deep error", null, "System.Exception"), "deep-failed")
        };

        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            existingEffects,
            new InMemoryFunctionStore().EffectsStore,
            DefaultSerializer.Instance,
            storageSession: null,
            clearChildren: false
        );

        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowTimeouts());
        var output = effect.ExecutionTree();

        var expected =
            "└─ ✓ [1] root\n" +
            "   └─ ⋯ [2]\n" +  // Missing [1, 2]
            "      └─ ⋯ [3]\n" +  // Missing [1, 2, 3]
            "         └─ ✗ [4] deep-failed (System.Exception)\n";
        StripAnsiColors(output).ShouldBe(expected);
    }
}
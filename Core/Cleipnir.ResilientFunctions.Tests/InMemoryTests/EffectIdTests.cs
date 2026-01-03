using Cleipnir.ResilientFunctions.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class EffectIdTests
{
    [TestMethod]
    public void EffectIdWithStateCanBeDeserialized()
    {
        var effectId = new EffectId([1]);
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void EffectIdWithContextCanBeDeserialized()
    {
        var parentEffect = new EffectId([2, 1]);
        var effectId = new EffectId([parentEffect.Id, 3]);
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void EffectIdWithContextAndEscapedCharactersCanBeDeserialized()
    {
        var parentEffect = new EffectId([1]);
        var effectId = new EffectId([parentEffect.Id, 2]);
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void EffectIdWithBackslashIsSerializedCorrectly()
    {
        var effectId = new EffectId([1]);
        var serializedId = effectId.Serialize();
        serializedId.Value.ShouldBe([1]);
        var deserializedId = EffectId.Deserialize(serializedId);
        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void EffectIdWithDotIsSerializedCorrectly()
    {
        var effectId = new EffectId([2]);
        var serializedId = effectId.Serialize();
        serializedId.Value.ShouldBe([2]);
        var deserializedId = EffectId.Deserialize(serializedId);
        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void EffectIdWithoutStateCanBeDeserialized()
    {
        var effectId = new EffectId([1]);
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void EffectIdWithEmptyIdAndContextCanBeDeserialized()
    {
        var effectId = new EffectId([0]);
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdChildWorks()
    {
        var effectId = new EffectId([2,3,4]);
        effectId.IsDescendant(new EffectId([1])).ShouldBeFalse();
        effectId.IsDescendant(new EffectId([2])).ShouldBeTrue();
        effectId.IsDescendant(new EffectId([2,2])).ShouldBeFalse();
        effectId.IsDescendant(new EffectId([2,3])).ShouldBeTrue();
        effectId.IsDescendant(new EffectId([2,3,3])).ShouldBeFalse();
        effectId.IsDescendant(new EffectId([2,3,4])).ShouldBeFalse();

        effectId.IsDescendant(new EffectId([2,3,4,1])).ShouldBeFalse();
    }

    [TestMethod]
    public void EffectIdIsChildWorks()
    {
        var effectId = new EffectId([2,3,4]);
        effectId.IsChild(new EffectId([1])).ShouldBeFalse();
        effectId.IsChild(new EffectId([2])).ShouldBeFalse();
        effectId.IsChild(new EffectId([2,2])).ShouldBeFalse();
        effectId.IsChild(new EffectId([2,3])).ShouldBeFalse();
        effectId.IsChild(new EffectId([2,3,3])).ShouldBeFalse();
        effectId.IsChild(new EffectId([2,3,4])).ShouldBeFalse();
        effectId.IsChild(new EffectId([2,3,4,5])).ShouldBeTrue();
        effectId.IsChild(new EffectId([2,3,4,5,6])).ShouldBeFalse();

        // Test with different parent contexts
        var parent = new EffectId([1]);
        parent.IsChild(new EffectId([1,2])).ShouldBeTrue();
        parent.IsChild(new EffectId([1,3])).ShouldBeTrue();
        parent.IsChild(new EffectId([2,1])).ShouldBeFalse();
        parent.IsChild(new EffectId([1])).ShouldBeFalse();
    }
}

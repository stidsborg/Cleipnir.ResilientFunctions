using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class EffectIdTests
{
    [TestMethod]
    public void EffectIdWithStateCanBeDeserialized()
    {
        var effectId = new EffectId(1, Context: Array.Empty<int>());
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithContextCanBeDeserialized()
    {
        var parentEffect = new EffectId(1, Context: [2]);
        var effectId = new EffectId(3, Context: [parentEffect.Id]);
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithContextAndEscapedCharactersCanBeDeserialized()
    {
        var parentEffect = new EffectId(1, Context: Array.Empty<int>());
        var effectId = new EffectId(2, Context: [parentEffect.Id]);
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithBackslashIsSerializedCorrectly()
    {
        var effectId = new EffectId(1, Context: Array.Empty<int>());
        var serializedId = effectId.Serialize();
        serializedId.Value.ShouldBe([1]);
        var deserializedId = EffectId.Deserialize(serializedId);
        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithDotIsSerializedCorrectly()
    {
        var effectId = new EffectId(2, Context: Array.Empty<int>());
        var serializedId = effectId.Serialize();
        serializedId.Value.ShouldBe([2]);
        var deserializedId = EffectId.Deserialize(serializedId);
        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithoutStateCanBeDeserialized()
    {
        var effectId = new EffectId(1, Context: Array.Empty<int>());
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void EffectIdWithEmptyIdAndContextCanBeDeserialized()
    {
        var effectId = new EffectId(0, Context: Array.Empty<int>());
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }
}
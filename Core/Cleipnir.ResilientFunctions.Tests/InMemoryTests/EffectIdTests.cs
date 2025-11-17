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
        var effectId = new EffectId("SomeValue", "");
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void EffectIdWithContextCanBeDeserialized()
    {
        var parentEffect = new EffectId("SomeParentId", "ESomeParentContext");
        var effectId = new EffectId("SomeValue", parentEffect.Serialize().Value);
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void EffectIdWithContextAndEscapedCharactersCanBeDeserialized()
    {
        var parentEffect = new EffectId("SomeParentId", "");
        var effectId = new EffectId("Some.Value\\WithBackSlash", parentEffect.Serialize().Value);
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void EffectIdWithBackslashIsSerializedCorrectly()
    {
        var effectId = new EffectId("\\", "");
        var serializedId = effectId.Serialize();
        serializedId.Value.ShouldBe("E\\\\");
        var deserializedId = EffectId.Deserialize(serializedId);
        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void EffectIdWithDotIsSerializedCorrectly()
    {
        var effectId = new EffectId(".", "");
        var serializedId = effectId.Serialize();
        serializedId.Value.ShouldBe("E\\.");
        var deserializedId = EffectId.Deserialize(serializedId);
        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void EffectIdWithoutStateCanBeDeserialized()
    {
        var effectId = new EffectId("SomeValue", "");
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void StoredEffectIdIsBasedOnSerializedEffectIdValue()
    {
        var effectId = new EffectId("SomeId", new EffectId("SomeParentId", "ESomeParentContext").Serialize().Value);
        var serializedEffectId = effectId.Serialize();

        var storedEffectId = effectId.ToStoredEffectId();
        storedEffectId.Value.ShouldBe(StoredIdFactory.FromString(serializedEffectId.Value));
    }

    [TestMethod]
    public void EffectIdWithEmptyIdAndContextCanBeDeserialized()
    {
        var effectId = new EffectId("", "");
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }
}
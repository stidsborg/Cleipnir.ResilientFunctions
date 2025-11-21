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
        var effectId = new EffectId("SomeValue", Context: "");
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithContextCanBeDeserialized()
    {
        var parentEffect = new EffectId("SomeParentId", Context: "ESomeParentContext");
        var effectId = new EffectId("SomeValue", Context: parentEffect.Serialize().Value);
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithContextAndEscapedCharactersCanBeDeserialized()
    {
        var parentEffect = new EffectId("SomeParentId", Context: "");
        var effectId = new EffectId("Some.Value\\WithBackSlash", Context: parentEffect.Serialize().Value);
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithBackslashIsSerializedCorrectly()
    {
        var effectId = new EffectId("\\", Context: "");
        var serializedId = effectId.Serialize();
        serializedId.Value.ShouldBe("\\\\");
        var deserializedId = EffectId.Deserialize(serializedId);
        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithDotIsSerializedCorrectly()
    {
        var effectId = new EffectId(".", Context: "");
        var serializedId = effectId.Serialize();
        serializedId.Value.ShouldBe("\\.");
        var deserializedId = EffectId.Deserialize(serializedId);
        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithoutStateCanBeDeserialized()
    {
        var effectId = new EffectId("SomeValue", Context: "");
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void StoredEffectIdIsBasedOnSerializedEffectIdValue()
    {
        var effectId = new EffectId("SomeId", Context: new EffectId("SomeParentId", Context: "ESomeParentContext").Serialize().Value);
        var serializedEffectId = effectId.Serialize();

        var storedEffectId = effectId.ToStoredEffectId();
        storedEffectId.Value.ShouldBe(StoredIdFactory.FromString(serializedEffectId.Value));
    }

    [TestMethod]
    public void EffectIdWithEmptyIdAndContextCanBeDeserialized()
    {
        var effectId = new EffectId("0", Context: "");
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);

        deserializedId.ShouldBe(effectId);
    }
}
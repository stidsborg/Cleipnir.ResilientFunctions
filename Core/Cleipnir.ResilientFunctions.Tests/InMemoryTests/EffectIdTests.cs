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
        var effectId = new EffectId("SomeValue", EffectType.State, Context: "");
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);
        
        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithContextCanBeDeserialized()
    {
        var parentEffect = new EffectId("SomeParentId", EffectType.Effect, Context: "ESomeParentContext");
        var effectId = new EffectId("SomeValue", EffectType.State, Context: parentEffect.Serialize());
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);
        
        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithContextAndEscapedCharactersCanBeDeserialized()
    {
        var parentEffect = new EffectId("SomeParentId", EffectType.Effect, Context: "");
        var effectId = new EffectId("Some.Value\\WithBackSlash", EffectType.State, Context: parentEffect.Serialize());
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);
        
        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithBackslashIsSerializedCorrectly()
    {
        var effectId = new EffectId("\\", EffectType.State, Context: "");
        var serializedId = effectId.Serialize();
        serializedId.ShouldBe("S\\\\");
        var deserializedId = EffectId.Deserialize(serializedId);
        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithDotIsSerializedCorrectly()
    {
        var effectId = new EffectId(".", EffectType.State, Context: "");
        var serializedId = effectId.Serialize();
        serializedId.ShouldBe("S\\.");
        var deserializedId = EffectId.Deserialize(serializedId);
        deserializedId.ShouldBe(effectId);
    }
    
    [TestMethod]
    public void EffectIdWithoutStateCanBeDeserialized()
    {
        var effectId = new EffectId("SomeValue", EffectType.Effect, Context: "");
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);
        
        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void StoredEffectIdIsBasedOnSerializedEffectIdValue()
    {
        var effectId = new EffectId("SomeId", EffectType.Effect, Context: new EffectId("SomeParentId", EffectType.Effect, Context: "ESomeParentContext").Serialize());
        var serializedEffectId = effectId.Serialize();

        var storedEffectId = effectId.ToStoredEffectId();
        storedEffectId.Value.ShouldBe(StoredIdFactory.FromString(serializedEffectId));
    }
    
    [TestMethod]
    public void EffectIdWithEmptyIdAndContextCanBeDeserialized()
    {
        var effectId = new EffectId("", EffectType.State, Context: "");
        var serializedId = effectId.Serialize();
        var deserializedId = EffectId.Deserialize(serializedId);
        
        deserializedId.ShouldBe(effectId);
    }
}
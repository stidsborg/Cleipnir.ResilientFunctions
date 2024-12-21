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
        var effectId = new EffectId("SomeValue", IsState: true);
        var deserializedId = EffectId.Deserialize(effectId.Serialize());
        
        deserializedId.ShouldBe(effectId); ;
    }
    
    [TestMethod]
    public void EffectIdWithoutStateCanBeDeserialized()
    {
        var effectId = new EffectId("SomeValue", IsState: false);
        var deserializedId = EffectId.Deserialize(effectId.Serialize());
        
        deserializedId.ShouldBe(effectId);
    }

    [TestMethod]
    public void StoredEffectIdIsBasedOnSerializedEffectIdValue()
    {
        var effectId = new EffectId("SomeValue", IsState: false);
        var serializedEffectId = effectId.Serialize();

        var storedEffectId = effectId.ToStoredEffectId();
        storedEffectId.Value.ShouldBe(StoredIdFactory.FromString(serializedEffectId));
    }
}
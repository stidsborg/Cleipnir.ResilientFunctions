using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.UtilsTests;

[TestClass]
public class StoredIdTests
{
    [TestMethod]
    public void ToGuidIsSameForTwoEqualStoredIds()
    {
        var id1 = new StoredId("Instance#1".ToStoredInstance(1.ToStoredType()));
        var id2 = new StoredId("Instance#1".ToStoredInstance(1.ToStoredType()));
        var id3 = new StoredId("Instance#1".ToStoredInstance(3.ToStoredType()));
        var id4 = new StoredId("Instance#2".ToStoredInstance(4.ToStoredType()));

        id1.ToGuid().ShouldBe(id2.ToGuid());
        id1.ToGuid().ShouldNotBe(id3.ToGuid());
        id1.ToGuid().ShouldNotBe(id4.ToGuid());
    }
    
    [TestMethod]
    public void DifferentStringsHashToDifferentGuids()
    {
        var hash1 = StoredIdFactory.FromString("Test123");
        var hash2 = StoredIdFactory.FromString("Test124");
        
        hash1.ShouldNotBe(hash2);
    }
    
    [TestMethod]
    public void EqualStringsHashToSameGuids()
    {
        var hash1 = StoredIdFactory.FromString("Test123");
        var hash2 = StoredIdFactory.FromString("Test123");
        
        hash1.ShouldBe(hash2);
    }
    
    [TestMethod]
    public void DifferentIntsHashToDifferentGuids()
    {
        var hash1 = StoredIdFactory.FromInt(1);
        var hash2 = StoredIdFactory.FromInt(2);
        
        hash1.ShouldNotBe(hash2);
    }
    
    [TestMethod]
    public void EqualIntsHashToSameGuids()
    {
        var hash1 = StoredIdFactory.FromInt(1);
        var hash2 = StoredIdFactory.FromInt(1);
        
        hash1.ShouldBe(hash2);
    }
    
    [TestMethod]
    public void DifferentLongsHashToDifferentGuids()
    {
        var hash1 = StoredIdFactory.FromLong(1L);
        var hash2 = StoredIdFactory.FromLong(2L);
        
        hash1.ShouldNotBe(hash2);
    }
    
    [TestMethod]
    public void EqualLongsHashToSameGuids()
    {
        var hash1 = StoredIdFactory.FromLong(1L);
        var hash2 = StoredIdFactory.FromLong(1L);
        
        hash1.ShouldBe(hash2);
    }
    
    [TestMethod]
    public void TypeCanBeExtractedFromId()
    {
        var id = StoredInstance.Create("SomeInstanceId", new StoredType(1));
        id.StoredType.Value.ShouldBe(1);
        
        var id2 = StoredInstance.Create("SomeInstanceId", new StoredType(2));
        id2.StoredType.Value.ShouldBe(2);
        
        id2.Value.ShouldNotBe(id.Value);
    }
}
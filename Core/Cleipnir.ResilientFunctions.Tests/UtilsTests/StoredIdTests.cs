using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.UtilsTests;

[TestClass]
public class StoredIdTests
{
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
}
using System;
using Cleipnir.ResilientFunctions.Helpers;
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
        var id1 = StoredId.Create(1.ToUshort().ToStoredType(), "Instance#1");
        var id2 = StoredId.Create(1.ToUshort().ToStoredType(), "Instance#1");
        var id3 = StoredId.Create(3.ToUshort().ToStoredType(), "Instance#1");
        var id4 = StoredId.Create(4.ToUshort().ToStoredType(), "Instance#2");

        id1.ShouldBe(id2);
        id1.ShouldNotBe(id3);
        id1.ShouldNotBe(id4);
    }
    
    [TestMethod]
    public void TypeCanBeExtractedFromId()
    {
        var id = StoredId.Create(new StoredType(1), "SomeInstanceId");
        id.Type.Value.ShouldBe(1.ToUshort());
        
        var id2 = StoredId.Create(new StoredType(2), "SomeInstanceId");
        id2.Type.Value.ShouldBe(2.ToUshort());
        
        id2.ShouldNotBe(id);
    }
    
    [TestMethod]
    public void TypeCanBeExtractedFromGuid()
    {
        var id = "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF".ToGuid();
        var type = 25.ToUshort();
        var bytes = id.ToByteArray();
        
        BitConverter.GetBytes(type).CopyTo(bytes, index: 0);

        var id2 = new Guid(bytes);
        id2.ToString().ShouldBe("ffff0019-ffff-ffff-ffff-ffffffffffff");

        var storedId = new StoredId(id2);
        storedId.Type.Value.ShouldBe(25.ToUshort());
        storedId.AsGuid.ShouldBe(id2);
    }
}
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.UtilsTests;

[TestClass]
public class ByteArrayMarshallerTests
{
    [TestMethod]
    public void MultipleStringsCanBeMarshalledAndReconstructed()
    {
        const string str1 = "hello world";
        const string str2 = "hello universe";
        const string str3 = "testing tester";
        const string str4 = "";
        const byte[]? str5 = null;
        const string str6 = "|";
        
        
        var marshalled = ByteArrayMarshaller.Serialize(str1.ToUtf8Bytes(), str2.ToUtf8Bytes(), str3.ToUtf8Bytes(), str4.ToUtf8Bytes(), str5, str6.ToUtf8Bytes());
        var reconstructed = ByteArrayMarshaller.Deserialize(marshalled);
        reconstructed.Count.ShouldBe(6);
        reconstructed[0]!.Value.ToStringFromUtf8Bytes().ShouldBe(str1);
        reconstructed[1]!.Value.ToStringFromUtf8Bytes().ShouldBe(str2);
        reconstructed[2]!.Value.ToStringFromUtf8Bytes().ShouldBe(str3);
        reconstructed[3]!.Value.ToStringFromUtf8Bytes().ShouldBe(str4);
        reconstructed[4].ShouldBeNull();
        reconstructed[5]!.Value.ToStringFromUtf8Bytes().ShouldBe(str6);
    }
    
    [TestMethod]
    public void NothingCanBeMarshalledAndReconstructed()
    {
        var serialized = ByteArrayMarshaller.Serialize();
        var deserialized = ByteArrayMarshaller.Deserialize(serialized);
        deserialized.Count.ShouldBe(0);
    }
    
    [TestMethod]
    public void TwoSerializationParsesWorks()
    {
        const string str1 = "hello world";
        const string str2 = "hello universe";
        const string str3 = "testing tester";
        const string str4 = "";
        const byte[]? str5 = null;
        const string str6 = "|";

        var marshalled1 = ByteArrayMarshaller.Serialize(str1.ToUtf8Bytes(), str2.ToUtf8Bytes());
        var marshalled2 = ByteArrayMarshaller.Serialize(str3.ToUtf8Bytes(), str4.ToUtf8Bytes(), str5, str6.ToUtf8Bytes());
        var marshalled = ByteArrayMarshaller.Serialize(marshalled1, marshalled2);
        var reconstructed = ByteArrayMarshaller.Deserialize(marshalled);
        reconstructed.Count.ShouldBe(2);
        var reconstructed1 = ByteArrayMarshaller.Deserialize(reconstructed[0]!.Value.Span.ToArray());
        var reconstructed2 = ByteArrayMarshaller.Deserialize(reconstructed[1]!.Value.Span.ToArray());
        
        reconstructed1.Count.ShouldBe(2);
        reconstructed1[0]!.Value.ToStringFromUtf8Bytes().ShouldBe(str1);
        reconstructed1[1]!.Value.ToStringFromUtf8Bytes().ShouldBe(str2);
        
        reconstructed2.Count.ShouldBe(4);
        reconstructed2[0]!.Value.ToStringFromUtf8Bytes().ShouldBe(str3);
        reconstructed2[1]!.Value.ToStringFromUtf8Bytes().ShouldBe(str4);
        reconstructed2[2].ShouldBeNull();
        reconstructed2[3]!.Value.ToStringFromUtf8Bytes().ShouldBe(str6);
    }
}
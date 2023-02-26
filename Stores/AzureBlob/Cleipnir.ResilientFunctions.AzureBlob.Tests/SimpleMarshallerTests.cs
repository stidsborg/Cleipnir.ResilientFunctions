using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests;

[TestClass]
public class SimpleMarshallerTests
{
    [TestMethod]
    public void MultipleStringsCanBeMarshalledAndReconstructed()
    {
        const string str1 = "hello world";
        const string str2 = "hello universe";
        const string str3 = "testing tester";
        const string str4 = "";
        const string str5 = "|";

        var marshalledString = SimpleMarshaller.Serialize(str1, str2, str3, str4, str5);
        var reconstructed = SimpleMarshaller.Deserialize(marshalledString);
        reconstructed.Count.ShouldBe(5);
        reconstructed[0].ShouldBe(str1);
        reconstructed[1].ShouldBe(str2);
        reconstructed[2].ShouldBe(str3);
        reconstructed[3].ShouldBe(str4);
        reconstructed[4].ShouldBe(str5);
    }
    
    [TestMethod]
    public void EmptyStringCanBeMarshalledAndReconstructed()
    {
        var marshalledString = SimpleMarshaller.Serialize("");
        var reconstructed = SimpleMarshaller.Deserialize(marshalledString);
        reconstructed.Count.ShouldBe(1);
        reconstructed[0].ShouldBe("");
    }
    
    [TestMethod]
    public void NoStringStringCanBeMarshalledAndReconstructed()
    {
        var marshalledString = SimpleMarshaller.Serialize();
        var reconstructed = SimpleMarshaller.Deserialize(marshalledString);
        reconstructed.Count.ShouldBe(0);
    }

    [TestMethod]
    public void NullStringCanBeMarshalledAndReconstructed()
    {
        const string str1 = "hello world";
        const string? str2 = null;
        const string str3 = "hello universe";

        var marshalledString = SimpleMarshaller.Serialize(str1, str2, str3);
        var reconstructed = SimpleMarshaller.Deserialize(marshalledString);
        reconstructed.Count.ShouldBe(3);
        reconstructed[0].ShouldBe(str1);
        reconstructed[1].ShouldBe(str2);
        reconstructed[2].ShouldBe(str3);
    }

    [TestMethod]
    public void NewMarshalledStringCanBeAppendedToExistingMarshalledString()
    {
        const string str1 = "hello world";
        const string str2 = "hello universe";
        const string str3 = "testing tester";
        const string str4 = "";
        const string str5 = "|";

        var marshalledString = $"{SimpleMarshaller.Serialize(str1, str2, str3, str4)}{SimpleMarshaller.Serialize(str5)}";
        
        var reconstructed = SimpleMarshaller.Deserialize(marshalledString);
        reconstructed.Count.ShouldBe(5);
        reconstructed[0].ShouldBe(str1);
        reconstructed[1].ShouldBe(str2);
        reconstructed[2].ShouldBe(str3);
        reconstructed[3].ShouldBe(str4);
        reconstructed[4].ShouldBe(str5);
    }
}
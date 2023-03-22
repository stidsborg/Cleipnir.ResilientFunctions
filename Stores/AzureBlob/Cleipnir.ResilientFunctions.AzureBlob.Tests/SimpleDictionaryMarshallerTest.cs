using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests;

[TestClass]
public class SimpleDictionaryMarshallerTest
{
    [TestMethod]
    public void MultipleStringsCanBeMarshalledAndReconstructed()
    {
        var dictionary = new Dictionary<string, string?>
        {
            {"First", "hello world"},
            {"Second", "hello universe"},
            {"Testing", "testing tester"},
            {"", "|"}
        };
        var marshalledString = SimpleDictionaryMarshaller.Serialize(dictionary);
        var reconstructed = SimpleDictionaryMarshaller.Deserialize(marshalledString);

        dictionary.Count.ShouldBe(reconstructed.Count);
        foreach (var (key, value) in dictionary)
            reconstructed[key].ShouldBe(value);
    }
    
    [TestMethod]
    public void NoStringStringCanBeMarshalledAndReconstructed()
    {
        var marshalledString = SimpleDictionaryMarshaller.Serialize(new Dictionary<string, string?>());
        var reconstructed = SimpleMarshaller.Deserialize(marshalledString);
        reconstructed.Count.ShouldBe(0);
    }

    [TestMethod]
    public void NewMarshalledStringCanBeAppendedToExistingMarshalledString()
    {
        var dictionary1 = new Dictionary<string, string?>
        {
            {"First", "hello world"},
            {"Second", "hello universe"},
        };
        var dictionary2 = new Dictionary<string, string?>
        {
            {"Second", "hello multiverse"},
        };
        var marshalledString = $"{SimpleDictionaryMarshaller.Serialize(dictionary1)}{SimpleDictionaryMarshaller.Serialize(dictionary2)}";
        
        var reconstructed = SimpleDictionaryMarshaller.Deserialize(marshalledString);
        reconstructed.Count.ShouldBe(2);
        dictionary1["First"].ShouldBe("hello world");
        dictionary2["Second"].ShouldBe("hello multiverse");
    }
    
    [TestMethod]
    public void NullStringCanBeMarshalledAndReconstructed()
    {
        var dictionary = new Dictionary<string, string?>
        {
            { "First", "hello world" },
            { "Second", null },
            { "Third", "hello universe" }
        };

        var marshalledString = SimpleDictionaryMarshaller.Serialize(dictionary);
        var reconstructed = SimpleDictionaryMarshaller.Deserialize(marshalledString);
        reconstructed.Count.ShouldBe(3);
        reconstructed["First"].ShouldBe("hello world");
        reconstructed["Second"].ShouldBeNull();
        reconstructed["Third"].ShouldBe("hello universe");
    }
}
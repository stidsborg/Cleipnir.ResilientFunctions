using Cleipnir.ResilientFunctions.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.UtilsTests;

[TestClass]
public class KeyEncoderTests
{
    [TestMethod]
    public void NonEmptyGroupAndInstancePairCanBeEncodedAndDecoded()
    {
        const string group = "abc";
        const string instance = "defg";

        var encoded = KeyEncoder.Encode(group, instance);

        var (decodedGroup, decodedInstance) = KeyEncoder.Decode(encoded);
        decodedGroup.ShouldBe(group);
        decodedInstance.ShouldBe(instance);
    }
    
    [TestMethod]
    public void EmptyGroupAndNonEmptyInstancePairCanBeEncodedAndDecoded()
    {
        const string group = "";
        const string instance = "defg";

        var encoded = KeyEncoder.Encode(group, instance);

        var (decodedGroup, decodedInstance) = KeyEncoder.Decode(encoded);
        decodedGroup.ShouldBe(group);
        decodedInstance.ShouldBe(instance);
    }
    
    [TestMethod]
    public void EmptyGroupAndEmptyInstancePairCanBeEncodedAndDecoded()
    {
        const string group = "";
        const string instance = "";

        var encoded = KeyEncoder.Encode(group, instance);

        var (decodedGroup, decodedInstance) = KeyEncoder.Decode(encoded);
        decodedGroup.ShouldBe(group);
        decodedInstance.ShouldBe(instance);
    }
    
    [TestMethod]
    public void NonEmptyGroupAndNullInstancePairCanBeEncodedAndDecoded()
    { 
        const string group = "abc";
        string? instance = null;

        var encoded = KeyEncoder.Encode(group, instance);

        var (decodedGroup, decodedInstance) = KeyEncoder.Decode(encoded);
        decodedGroup.ShouldBe(group);
        decodedInstance.ShouldBe(instance);
    }
}
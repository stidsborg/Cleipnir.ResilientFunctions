using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
using Array = System.Array;

namespace Cleipnir.ResilientFunctions.Tests.UtilsTests;

[TestClass]
public class BinaryPackerTests
{
    [TestMethod]
    public void TwoSimpleByteArraysCanBeCombinedAndSplitAgain()
    {
        var arr1 = new byte[] { 101 };
        var arr2 = new byte[] { 201, 202, 203 };

        var packedArray = BinaryPacker.Pack(arr1, arr2);
        var reconstructed = BinaryPacker.Split(packedArray, pieces: 2);
        reconstructed.Count.ShouldBe(2);
        reconstructed[0].ShouldBe(arr1);
        reconstructed[1].ShouldBe(arr2);
    }
    
    [TestMethod]
    public void MultipleStringsCanBeMarshalledAndReconstructed()
    {
        const string str1 = "hello world";
        const string str2 = "hello universe";
        const string str3 = "testing tester";
        const string str4 = "";
        const string str5 = "|";

        var packedArray = BinaryPacker.Pack(str1.ToUtf8Bytes(), str2.ToUtf8Bytes(), str3.ToUtf8Bytes(), str4.ToUtf8Bytes(), str5.ToUtf8Bytes());
        var reconstructed = BinaryPacker.Split(packedArray, pieces: 5);
        reconstructed.Count.ShouldBe(5);
        reconstructed[0]!.ToStringFromUtf8Bytes().ShouldBe(str1);
        reconstructed[1]!.ToStringFromUtf8Bytes().ShouldBe(str2);
        reconstructed[2]!.ToStringFromUtf8Bytes().ShouldBe(str3);
        reconstructed[3]!.ToStringFromUtf8Bytes().ShouldBe(str4);
        reconstructed[4]!.ToStringFromUtf8Bytes().ShouldBe(str5);
    }
    
    [TestMethod]
    public void EmptyStringCanBeMarshalledAndReconstructed()
    {
        var empty = Array.Empty<byte>();
        var combined = BinaryPacker.Pack(empty);
        combined.Length.ShouldNotBe(0);
        var split = BinaryPacker.Split(combined, pieces: 1)[0];
        split.ShouldBe(empty);
    }
    
    [TestMethod]
    public void NullCanBeMarshalledAndReconstructed()
    {
        var first = new byte[] { 100, 101 };
        var last = new byte[] { 200, 201 };
        var combined = BinaryPacker.Pack(first, null, last);
        
        var split = BinaryPacker.Split(combined, pieces: 3);
        split[0].ShouldBe(first);
        split[1].ShouldBeNull();
        split[2].ShouldBe(last);
    }
    
    [TestMethod]
    public void MultipleStringsCanBeAppendedToSourceAndAllReconstructed()
    {
        const string str1 = "hello world";
        const string str2 = "hello universe";
        
        var sourceArray = BinaryPacker.Pack(str1.ToUtf8Bytes(), str2.ToUtf8Bytes());        
        
        const string str3 = "testing tester";
        const string str4 = "";
        const string str5 = "|";

        var appended = BinaryPacker.Append(sourceArray, str3.ToUtf8Bytes(), str4.ToUtf8Bytes(), str5.ToUtf8Bytes());
        
        var reconstructed = BinaryPacker.Split(appended, pieces: 5);
        reconstructed.Count.ShouldBe(5);
        reconstructed[0]!.ToStringFromUtf8Bytes().ShouldBe(str1);
        reconstructed[1]!.ToStringFromUtf8Bytes().ShouldBe(str2);
        reconstructed[2]!.ToStringFromUtf8Bytes().ShouldBe(str3);
        reconstructed[3]!.ToStringFromUtf8Bytes().ShouldBe(str4);
        reconstructed[4]!.ToStringFromUtf8Bytes().ShouldBe(str5);
    }
    
    [TestMethod]
    public void NullCanBeAppendedToSourceAndAllReconstructed()
    {
        const string str1 = "hello world";
        const string str2 = "hello universe";
        
        var sourceArray = BinaryPacker.Pack(str1.ToUtf8Bytes(), str2.ToUtf8Bytes());        
        var appended = BinaryPacker.Append(sourceArray, [null]);
        
        var reconstructed = BinaryPacker.Split(appended, pieces: 5);
        reconstructed.Count.ShouldBe(3);
        reconstructed[0]!.ToStringFromUtf8Bytes().ShouldBe(str1);
        reconstructed[1]!.ToStringFromUtf8Bytes().ShouldBe(str2);
        reconstructed[2].ShouldBeNull();
    }
}
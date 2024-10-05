using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.UtilsTests;

[TestClass]
public class EnumerableExtensionTests
{
    [TestMethod]
    public void ElementsAreEvenlyDistributed()
    {
        var numbers = new[] {1, 2, 3, 4, 5, 6, 7, 8};

        var buckets = numbers.Split(3);
        buckets.Count.ShouldBe(3);
        buckets[0].ShouldBe([1,2,3]);
        buckets[1].ShouldBe([4,5,6]);
        buckets[2].ShouldBe([7,8]);
    }
    
    [TestMethod]
    public void ElementsCanBeSplittedOverMoreBucketsThanElements()
    {
        var numbers = new[] {1, 2};

        var buckets = numbers.Split(3);
        buckets.Count.ShouldBe(3);
        buckets[0].ShouldBe([1]);
        buckets[1].ShouldBe([2]);
        buckets[2].ShouldBe([]);
    }
}
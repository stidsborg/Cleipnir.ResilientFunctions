using System;
using System.Collections.Generic;
using System.Linq;

namespace Cleipnir.ResilientFunctions.Helpers;

public static class EnumerableExtensions
{
    public static IReadOnlyList<IReadOnlyList<T>> Split<T>(this IEnumerable<T> elms, int buckets)
    {
        var elmsArr = elms.ToArray();
        
        var elmsPerBucket = elmsArr.Length / buckets;
        var leftOver = elmsArr.Length % buckets;
        var elmIndex = 0;
        var arrs = new T[buckets][];
        
        for (var i = 0; i < buckets; i++)
        {
            var size = leftOver > 0 ? elmsPerBucket + 1 : elmsPerBucket;
            var destinationArray = new T[size];
            Array.Copy(elmsArr, elmIndex, destinationArray, destinationIndex: 0, size);
            leftOver--;
            elmIndex += size;
            arrs[i] = destinationArray;
        }

        return arrs;
    }
}
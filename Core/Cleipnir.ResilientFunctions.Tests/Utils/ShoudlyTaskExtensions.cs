using System.Collections.Generic;
using System.Threading.Tasks;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class ShoudlyTaskExtensions
{
    public static async Task ShouldBeTrueAsync(this Task<bool> task) => (await task).ShouldBeTrue();
    public static async Task ShouldBeFalseAsync(this Task<bool> task) => (await task).ShouldBeFalse();

    public static async Task ShouldBeNullAsync<T>(this Task<T?> task) where T : class 
        => (await task).ShouldBeNull();

    public static async Task<T> ShouldNotBeNullAsync<T>(this Task<T?> task) where T : notnull
    {
        var result = await task;
        if (result == null)
            throw new ShouldAssertException("Awaited result was null");
        return result;
    } 

    public static async Task ShouldBeDefault<T>(this Task<T?> task)
        => (await task).ShouldBe(default(T));
    
    public static async Task ShouldNotBeDefault<T>(this Task<T> task) 
        => (await task).ShouldNotBe(default(T));
    
    public static async Task ShouldBeAsync<T>(this Task<T> task, T expected) 
        => (await task).ShouldBe(expected);

    public static async Task ShouldBeEmptyAsync<T>(this Task<IEnumerable<T>> task)
        => (await task).ShouldBeEmpty();
    public static async Task ShouldBeEmptyAsync<T>(this Task<IReadOnlyList<T>> task)
        => (await task).ShouldBeEmpty();
}
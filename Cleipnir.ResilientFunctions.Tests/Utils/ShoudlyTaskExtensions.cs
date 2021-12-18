using System.Threading.Tasks;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class ShoudlyTaskExtensions
{
    public static async Task ShouldBeTrueAsync(this Task<bool> task) => (await task).ShouldBeTrue();
    public static async Task ShouldBeFalseAsync(this Task<bool> task) => (await task).ShouldBeFalse();

    public static async Task ShouldBeNullAsync<T>(this Task<T?> task) where T : class 
        => (await task).ShouldBeNull();
    
    public static async Task ShouldNotBeNullAsync<T>(this Task<T> task) where T : class 
        => (await task).ShouldNotBeNull();
}
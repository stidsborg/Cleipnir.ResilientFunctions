using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils.Register;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.UtilsTests;

public abstract class RegisterTests
{
    public abstract Task SetValueWithNoExistingValueSucceeds();
    protected async Task SetValueWithNoExistingValueSucceeds(Task<IRegister> registerTask)
    {
        var register = await registerTask;
        var (group, key) = GetGroupAndKey();

        await register.Exists(group, key).ShouldBeFalseAsync();
        await register.SetIfEmpty(group, key, value: "hello world").ShouldBeTrueAsync();

        var value = await register.Get(group, key);
        value.ShouldBe("hello world");
        
        await register.Exists(group, key).ShouldBeTrueAsync();
    }
    
    public abstract Task SetValueIfEmptyFailsWhenRegisterHasExistingValue();
    protected async Task SetValueIfEmptyFailsWhenRegisterHasExistingValue(Task<IRegister> registerTask)
    {
        var register = await registerTask;
        var (group, key) = GetGroupAndKey();

        await register.SetIfEmpty(group, key, value: "hello world").ShouldBeTrueAsync();
        await register.SetIfEmpty(group, key, value: "hello universe").ShouldBeFalseAsync();
        
        var value = await register.Get(group, key);
        value.ShouldBe("hello world");
    }
    
    public abstract Task CompareAndSwapWithNoExistingValueSucceeds();
    protected async Task CompareAndSwapWithNoExistingValueSucceeds(Task<IRegister> registerTask)
    {
        var register = await registerTask;
        var (group, key) = GetGroupAndKey();

        await register.CompareAndSwap(group, key, newValue: "hello world", expectedValue: "", setIfEmpty: true).ShouldBeTrueAsync();

        var value = await register.Get(group, key);
        value.ShouldBe("hello world");
    }
    
    public abstract Task CompareAndSwapFailsWithNoExistingValue();
    protected async Task CompareAndSwapFailsWithNoExistingValue(Task<IRegister> registerTask)
    {
        var register = await registerTask;
        var (group, key) = GetGroupAndKey();

        await register.CompareAndSwap(group, key, newValue: "hello world", expectedValue: "", setIfEmpty: false).ShouldBeFalseAsync();

        await register.Exists(group, key).ShouldBeFalseAsync();
    }

    public abstract Task CompareAndSwapSucceedsIfAsExpected();
    protected async Task CompareAndSwapSucceedsIfAsExpected(Task<IRegister> registerTask)
    {
        var register = await registerTask;
        var (group, key) = GetGroupAndKey();

        await register.SetIfEmpty(group, key, value: "hello world").ShouldBeTrueAsync();

        await register.CompareAndSwap(group, key, newValue: "hello universe", expectedValue: "hello world");
        await register.SetIfEmpty(group, key, value: "hello universe").ShouldBeFalseAsync();
        
        var value = await register.Get(group, key);
        value.ShouldBe("hello universe");
    }
    
    public abstract Task CompareAndSwapSucceedsIfAsExpectedIgnoreIfNoExisting();
    protected async Task CompareAndSwapSucceedsIfAsExpectedIgnoreIfNoExisting(Task<IRegister> registerTask)
    {
        var register = await registerTask;
        var (group, key) = GetGroupAndKey();

        await register.SetIfEmpty(group, key, value: "hello world").ShouldBeTrueAsync();

        await register.CompareAndSwap(group, key, newValue: "hello universe", expectedValue: "hello world", setIfEmpty: false);
        await register.SetIfEmpty(group, key, value: "hello universe").ShouldBeFalseAsync();
        
        var value = await register.Get(group, key);
        value.ShouldBe("hello universe");
    }

    public abstract Task ExistsIfFalseForNonExistingRegister();
    protected async Task ExistsIfFalseForNonExistingRegister(Task<IRegister> registerTask)
    {
        var register = await registerTask;
        var (group, key) = GetGroupAndKey();

        await register.Exists(group, key).ShouldBeFalseAsync();
    }
    
    public abstract Task ExistingValueIsNullForNonExistingRegister();
    protected async Task ExistingValueIsNullForNonExistingRegister(Task<IRegister> registerTask)
    {
        var register = await registerTask;
        var (group, key) = GetGroupAndKey();

        await register.Get(group, key).ShouldBeNullAsync();
    }

    public abstract Task DeleteSucceedsForNonExistingRegister();
    protected async Task DeleteSucceedsForNonExistingRegister(Task<IRegister> registerTask)
    {
        var register = await registerTask;
        var (group, key) = GetGroupAndKey();

        await register.Delete(group, key);
    }
    
    public abstract Task DeleteSucceedsForExistingRegister();
    protected async Task DeleteSucceedsForExistingRegister(Task<IRegister> registerTask)
    {
        var register = await registerTask;
        var (group, key) = GetGroupAndKey();

        await register.SetIfEmpty(group, key, value: "hello world").ShouldBeTrueAsync();
        await register.Delete(group, key);

        await register.Exists(group, key).ShouldBeFalseAsync();
    }
    
    public abstract Task DeleteSucceedsWithExpectedValueForExistingRegister();
    protected async Task DeleteSucceedsWithExpectedValueForExistingRegister(Task<IRegister> registerTask)
    {
        var register = await registerTask;
        var (group, key) = GetGroupAndKey();

        await register.SetIfEmpty(group, key, value: "hello world").ShouldBeTrueAsync();
        await register.Delete(group, key, "hello world").ShouldBeTrueAsync();

        await register.Exists(group, key).ShouldBeFalseAsync();
    }
    
    public abstract Task DeleteFailsWhenNonExpectedValueForExistingRegister();
    protected async Task DeleteFailsWhenNonExpectedValueForExistingRegister(Task<IRegister> registerTask)
    {
        var register = await registerTask;
        var (group, name) = GetGroupAndKey();

        await register.SetIfEmpty(group, name, value: "hello world").ShouldBeTrueAsync();
        await register.Delete(group, name, "hello universe").ShouldBeFalseAsync();

        await register.Exists(group, name).ShouldBeTrueAsync();
    }
    
    private record GroupAndKey(string Group, string Key);
    private static GroupAndKey GetGroupAndKey([CallerMemberName] string memberName = "") =>
        new(Group: nameof(RegisterTests), Key: memberName);
}
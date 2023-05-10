using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils.Register;

public class Register : IRegister
{
    private readonly IUnderlyingRegister _underlyingRegister;

    public Register(IUnderlyingRegister underlyingRegister) => _underlyingRegister = underlyingRegister;

    public Task<bool> SetIfEmpty(string group, string name, string value)
        => _underlyingRegister.SetIfEmpty(RegisterType.Register, group, name, value);

    public Task<bool> CompareAndSwap(string group, string name, string newValue, string expectedValue, bool setIfEmpty = true)
        => _underlyingRegister.CompareAndSwap(RegisterType.Register, group, name, newValue, expectedValue, setIfEmpty);

    public Task<string?> Get(string group, string name)
        => _underlyingRegister.Get(RegisterType.Register, group, name);

    public Task<bool> Delete(string group, string name, string expectedValue)
        => _underlyingRegister.Delete(RegisterType.Register, group, name, expectedValue);

    public Task Delete(string group, string name)
        => _underlyingRegister.Delete(RegisterType.Register, group, name);

    public Task<bool> Exists(string group, string name)
        => _underlyingRegister.Exists(RegisterType.Register, group, name);
}
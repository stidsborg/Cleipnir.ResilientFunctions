using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils;

public interface IUnderlyingRegister
{
    Task<bool> SetIfEmpty(RegisterType registerType, string group, string name, string value);
    Task<bool> CompareAndSwap(RegisterType registerType, string group, string name, string newValue, string expectedValue, bool setIfEmpty = true);
    Task<string?> Get(RegisterType registerType, string group, string name);
    Task<bool> Delete(RegisterType registerType, string group, string name, string expectedValue);
    Task Delete(RegisterType registerType, string group, string name);
    Task<bool> Exists(RegisterType registerType, string group, string name);
}
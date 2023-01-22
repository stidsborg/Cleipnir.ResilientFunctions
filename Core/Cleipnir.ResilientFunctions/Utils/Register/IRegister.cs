using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils.Register;

public interface IRegister
{
    Task<bool> SetIfEmpty(string group, string key, string value);
    Task<bool> CompareAndSwap(string group, string key, string newValue, string expectedValue, bool setIfEmpty = true);
    Task<string?> Get(string group, string key);
    Task<bool> Delete(string group, string key, string expectedValue);
    Task Delete(string group, string key);
    Task<bool> Exists(string group, string key);
}
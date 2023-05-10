using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils.Register;

public interface IRegister
{
    Task<bool> SetIfEmpty(string group, string name, string value);
    Task<bool> CompareAndSwap(string group, string name, string newValue, string expectedValue, bool setIfEmpty = true);
    Task<string?> Get(string group, string name);
    Task<bool> Delete(string group, string name, string expectedValue);
    Task Delete(string group, string name);
    Task<bool> Exists(string group, string name);
}
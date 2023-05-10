using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils.Arbitrator;

public class Arbitrator : IArbitrator
{
    private readonly IUnderlyingRegister _register;

    public Arbitrator(IUnderlyingRegister underlyingRegister) => _register = underlyingRegister;

    public async Task<bool> Propose(string group, string name, string value)
        => await _register.CompareAndSwap(
            RegisterType.Arbitrator,
            group,
            name,
            newValue: value,
            expectedValue: value,
            setIfEmpty: true
        );

    public async Task Delete(string group, string name) 
        => await _register.Delete(RegisterType.Arbitrator, group, name);
}
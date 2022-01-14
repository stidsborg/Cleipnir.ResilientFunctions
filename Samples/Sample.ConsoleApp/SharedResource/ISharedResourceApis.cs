using System.Threading.Tasks;

namespace ConsoleApp.SharedResource;

public interface ISharedResourceApi1
{
    public Task SetValue(string resourceId, string value);
}

public interface ISharedResourceApi2
{
    public Task SetValue(string resourceId, string value);
}
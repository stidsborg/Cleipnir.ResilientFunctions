using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils.TagStore;

public interface ITagStore
{
    //Query
    public Task<IEnumerable<string>?> Get(string group);
    public Task<IEnumerable<string>?> Get(string group, string instance);

    //Mutations
    public Task Add(string group, IEnumerable<string> tags);
    public Task Add(string group, string instance, IEnumerable<string> tags);
    public Task Remove(string group, IEnumerable<string> tags);
    public Task Remove(string group, string instance, IEnumerable<string> tags);
    public Task RemoveAll(string group);
    public Task RemoveAll(string group, string instance);
    public Task Set(string group, string instance, IEnumerable<string> tags);
    public Task Set(string group, IEnumerable<string> tags);
    public Task Replace(string group, string oldTag, string newTag);
    public Task Replace(string group, string instance, string oldTag, string newTag);
}
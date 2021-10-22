using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Utils
{
    public static class TaskExtensions
    {
        public static async Task<List<T>> ToTaskList<T>(this Task<IEnumerable<T>> tasks) => new List<T>(await tasks);

        public static async Task<T[]> RandomlyPermutate<T>(this Task<IEnumerable<T>> tasks) 
            => (await tasks).RandomlyPermutate();
        
        public static Task<T> ToTask<T>(this T t) => Task.FromResult(t);
        public static List<T> ToList<T>(this T t) => new List<T> {t};
    }
}
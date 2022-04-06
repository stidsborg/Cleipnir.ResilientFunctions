using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public delegate Task<TReturn> InnerFunc<TParam, TReturn>(TParam param)
    where TParam : notnull;

public delegate Task<TReturn> InnerFunc<TParam, TScrapbook, TReturn>(TParam param, TScrapbook scrapbook)
    where TParam : notnull where TScrapbook : RScrapbook, new();
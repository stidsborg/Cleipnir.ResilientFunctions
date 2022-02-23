using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public delegate Task<RResult<TReturn>> InnerFunc<TParam, TReturn>(TParam param)
    where TParam : notnull;

public delegate Task<RResult<TReturn>> InnerFunc<TParam, TScrapbook, TReturn>(TParam param, TScrapbook scrapbook)
    where TParam : notnull where TScrapbook : RScrapbook;
    
public delegate Task<RResult> InnerAction<TParam>(TParam param)
    where TParam : notnull;
    
public delegate Task<RResult> InnerAction<TParam, TScrapbook>(TParam param, TScrapbook scrapbook)
    where TParam : notnull where TScrapbook : RScrapbook;
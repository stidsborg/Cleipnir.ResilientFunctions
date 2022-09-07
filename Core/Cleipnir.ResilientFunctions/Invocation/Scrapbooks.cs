using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Invocation;

public class Scrapbooks
{
    public RScrapbook ResilientFunctionScrapbook { get; }
    private readonly Dictionary<string, RScrapbook> _scrapbooks;

    public Scrapbooks(Dictionary<string, RScrapbook> scrapbooks, RScrapbook resilientFunctionScrapbook)
    {
        _scrapbooks = scrapbooks;
        ResilientFunctionScrapbook = resilientFunctionScrapbook;
    }

    public TScrapbook GetScrapbook<TScrapbook>(string owner) where TScrapbook : RScrapbook, new()
    {
        if (_scrapbooks.ContainsKey(owner)) return (TScrapbook)_scrapbooks[owner];
        
        var scrapbook = new TScrapbook();
        _scrapbooks[owner] = scrapbook;
        return scrapbook;
    }
}
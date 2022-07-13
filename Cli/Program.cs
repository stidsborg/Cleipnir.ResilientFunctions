using System.Diagnostics;

namespace Cleipnir.ResilientFunctions.Cli;

internal static class Program
{
    private static int Main(string[] args)
    {
        if (args.Length != 5 || args[0].ToLower() != "pack-all")
            return PrintUsageAndReturnError();

        var root = Path.GetFullPath(args[1]);
        var output = Path.GetFullPath(args[2]);
        var oldVersion = args[3];
        var newVersion = args[4];
        
        if (!Directory.Exists(root))
        {
            Console.WriteLine($"Source folder does not exist: {args}");
            return PrintUsageAndReturnError();
        }

        if (!Directory.Exists(output))
            Directory.CreateDirectory(output);

        Console.WriteLine($"Root path: {root}");
        Console.WriteLine($"Output path: {output}");

        Console.WriteLine("Projects: ");
        foreach (var projectPath in FindAllProjects(root).Where(IsPackageProject))
        {
            Console.WriteLine("Updating " + projectPath);
            Console.WriteLine("Project path: " + Path.GetDirectoryName(projectPath)!);
            UpdatePackageVersion(projectPath, oldVersion, newVersion);
            PackProject(Path.GetDirectoryName(projectPath)!, output);
        }

        return 0;
    }

    private static IEnumerable<string> FindAllProjects(string path)
    {
        var currPathProjects = Directory.GetFiles(path, "*.csproj");
        var subFolders = Directory.GetDirectories(path);
        return currPathProjects.Concat(subFolders.SelectMany(FindAllProjects)).ToList();
    }

    private static bool IsPackageProject(string path)
        => File.ReadAllText(path).Contains("<Version>");

    private static void UpdatePackageVersion(string path, string oldVersion, string newVersion)
    {
        var projectFileContent = File.ReadAllText(path);
        projectFileContent = projectFileContent.Replace(oldVersion, newVersion);
        File.WriteAllText(path, projectFileContent);
    }

    private static int PrintUsageAndReturnError()
    {
        Console.WriteLine("Usage: CRF pack-all <root> <output> <old_version> <new_version>");
        return -1;
    }

    private static void PackProject(string projectPath, string outputPath)
    {
        var p = new Process();
        p.StartInfo.RedirectStandardInput = true;
        p.StartInfo.RedirectStandardOutput = true;
        p.StartInfo.RedirectStandardError = true;
        p.StartInfo.UseShellExecute = false;
        p.StartInfo.FileName = @"C:\Program Files\dotnet\dotnet.exe";
        p.StartInfo.WorkingDirectory = projectPath;
        p.StartInfo.Arguments = $"dotnet pack -c Release /p:ContinuousIntegrationBuild=true -o {outputPath}";
        p.Start();

        while (!p.StandardOutput.EndOfStream)
            Console.WriteLine(p.StandardOutput.ReadLine());

        p.WaitForExit();
    }
}
using System.Diagnostics;

namespace Cleipnir.ResilientFunctions.Cli;

internal static class Program
{
    private static int Main(string[] args)
    {
        var root = Path.GetFullPath(@"C:\Repos\Cleipnir.ResilientFunctions");
        var output = Path.GetFullPath(@".\nugets");

        if (Directory.Exists(output))
            Directory.Delete(output, recursive: true);
        
        Directory.CreateDirectory(output);

        Console.WriteLine($"Root path: {root}");
        Console.WriteLine($"Output path: {output}");

        Console.WriteLine("Projects: ");
        foreach (var projectPath in FindAllProjects(root).Where(IsPackageProject))
        {
            Console.WriteLine("Updating " + projectPath);
            Console.WriteLine("Project path: " + Path.GetDirectoryName(projectPath)!);
            UpdatePackageVersion(projectPath);
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

    private static void UpdatePackageVersion(string path)
    {
        var projectFileContent = File.ReadAllText(path);
        projectFileContent = FindAndIncrementVersion(projectFileContent, path);
        File.WriteAllText(path, projectFileContent);
    }

    private static string FindAndIncrementVersion(string projectFileContent, string path)
    {
        var versionStartsAt = projectFileContent.IndexOf("<Version>", StringComparison.OrdinalIgnoreCase) + "<Version>".Length;
        var versionEndsAt = projectFileContent.IndexOf("</Version>", StringComparison.OrdinalIgnoreCase);
        var currentVersionString = projectFileContent[versionStartsAt..versionEndsAt];
        Console.WriteLine($"Current version: '{currentVersionString}' for path: '{path}'");
        
        var versionArray = currentVersionString.Split(".").Select(int.Parse).ToArray();
        versionArray[^1]++;
        var newVersionString = string.Join(".", versionArray);

        var newProjectFileContent = projectFileContent.Replace(currentVersionString, newVersionString);
        return newProjectFileContent;
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
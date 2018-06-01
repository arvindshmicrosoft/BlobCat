using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Microsoft.Azure.Samples.BlobCat
{
    class Program
    {
        /// <summary>
        /// This is the command line wrapper for the "BlobCat" utility. You can use it to concatenate a list of block blobs into a single block blob
        /// This scenario is commonly required in the Big Data space where there are typically multiple output files from ETL pipelines or queries
        /// and those files need to be "joined" together to make one large file.
        /// </summary>
        static int Main(string[] args)
        {
            var parseResult = CommandLine.Parser.Default.ParseArguments<ConcatBlobOptions, FilesToBlobOptions>(args)
                .MapResult(
                (ConcatBlobOptions opts) => {
                    return BlobCatEngine.BlobToBlob(
                        opts.SourceAccountName,
                        opts.SourceContainer,
                        opts.SourceKey,
                        opts.SourceFilePrefix,
                        opts.SortFilenames,
                        opts.SourceFiles.ToList(),
                        opts.DestAccountName,
                        opts.DestKey,
                        opts.DestContainer,
                        opts.DestFilename) ? 0 : 1;
                },
                (FilesToBlobOptions opts) => 
                {
                    return BlobCatEngine.DiskToBlob(
                        opts.SourceFolder,
                        opts.SourceFilePrefix,
                        opts.SortFilenames,
                        opts.SourceFiles.ToList(),
                        opts.DestAccountName,
                        opts.DestKey,
                        opts.DestContainer,
                        opts.DestFilename
                        ) ? 0 : 1;
                },
                errs => 1);

            return parseResult;
        }

        /// <summary>
        /// Helper method to parse the supplied command line. The expected format is given below:
        /// action /param1:value1 /param2:value2
        /// </summary>
        /// <returns></returns>
        private static Dictionary<string, string> ValidateCmdLine()
        {
            var cmdParams = new Dictionary<string, string>();
            var paramsRegex = new Regex(@"(?<paramname>\/[a-zA-Z]+?\:)(?<value>[^\/]+)");
            var actionRegex = new Regex(@"^(?<verb>\S+)\s+(?<restofcmdline>.+)");

            var cmdLine = System.Environment.CommandLine;

            Console.WriteLine(cmdLine);

            // firstly get rid of the current assembly's path, which is prefixed as the first parameter to dotnet.exe
            cmdLine = cmdLine.Replace(System.Reflection.Assembly.GetExecutingAssembly().Location, string.Empty).TrimStart();

            var verbmatch = actionRegex.Match(cmdLine);
            if (!verbmatch.Success)
            {
                return null;
            }

            // strip out the action verb and retain the rest of the cmd line
            cmdLine = actionRegex.Replace(cmdLine, "${restofcmdline}");

            var mc = paramsRegex.Matches(cmdLine);

            foreach (Match parammatch in mc)
            {
                if (cmdParams.ContainsKey(parammatch.Groups["paramname"].Value.Trim().ToUpper()))
                {
                    Console.WriteLine($"Duplicate value specified for {parammatch.Groups["paramname"].Value.Trim().ToUpper()}");
                    return null;
                }

                cmdParams.Add(
                    parammatch.Groups["paramname"].Value.Trim().ToUpper(),
                    parammatch.Groups["value"].Value.Trim()
                    );
            }

            return cmdParams;
        }
    }    
}

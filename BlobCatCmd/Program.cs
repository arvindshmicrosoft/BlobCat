//------------------------------------------------------------------------------
//<copyright company="Arvind Shyamsundar">
//    The MIT License (MIT)
//    
//    Copyright (c) 2018 Arvind Shyamsundar
//    
//    Permission is hereby granted, free of charge, to any person obtaining a copy
//    of this software and associated documentation files (the "Software"), to deal
//    in the Software without restriction, including without limitation the rights
//    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//    copies of the Software, and to permit persons to whom the Software is
//    furnished to do so, subject to the following conditions:
//    
//    The above copyright notice and this permission notice shall be included in all
//    copies or substantial portions of the Software.
//    
//    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//    SOFTWARE.
//
//    This sample code is not supported under any Microsoft standard support program or service. 
//    The entire risk arising out of the use or performance of the sample scripts and documentation remains with you. 
//    In no event shall Microsoft, its authors, or anyone else involved in the creation, production, or delivery of the scripts
//    be liable for any damages whatsoever (including, without limitation, damages for loss of business profits,
//    business interruption, loss of business information, or other pecuniary loss) arising out of the use of or inability
//    to use the sample scripts or documentation, even if Microsoft has been advised of the possibility of such damages.
//</copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Samples.BlobCat
{
    using CommandLine;
    using System.Linq;
    using Microsoft.Extensions.Logging;
    using System;
    using ShellProgressBar;
    using System.Reflection;
    using System.IO;

    class Program
    {
        /// <summary>
        /// This is the command line wrapper for the "BlobCat" utility. You can use it to concatenate a list of block blobs into a single block blob
        /// This scenario is commonly required in the Big Data space where there are typically multiple output files from ETL pipelines or queries
        /// and those files need to be "joined" together to make one large file.
        /// </summary>
        static int Main(string[] args)
        {
            // create a logger instance
            var logFilePath = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
                "log_{Date}.txt");

            var loggerFactory = new LoggerFactory().AddFile(logFilePath, args.Contains("--Debug") ? LogLevel.Debug : LogLevel.Information);

            var myLogger = loggerFactory.CreateLogger("BlobCatCmd");

            var parseResult = Parser.Default.ParseArguments<ConcatBlobOptions, FilesToBlobOptions>(args);

            ProgressBar pbar = null;

            // check for Console.IsInputRedirected || Console.IsOutputRedirected and if so then do not use progressbar
            var progress = new Progress<OpProgress>(opProgress =>
            {
                if (pbar is null)
                {
                    if (opProgress.TotalTicks > 0)
                    {
                        pbar = new ProgressBar(opProgress.TotalTicks, "Starting operation", new ProgressBarOptions
                        {
                            ProgressCharacter = '.',
                            ProgressBarOnBottom = true,
                            EnableTaskBarProgress = true,
                            DisplayTimeInRealTime = false
                        });
                    }
                }

                // Console.WriteLine(percent);
                if (pbar != null)
                {
                    pbar.Tick(opProgress.StatusMessage);
                }
                //myLogger.LogInformation($"Operation {opProgress.Percent}% complete; currently working on file {opProgress.StatusMessage}");
            });

            var retVal = parseResult.MapResult(
            (ConcatBlobOptions opts) =>
            {
                return BlobCatEngine.BlobToBlob(
                    opts.SourceAccountName,
                    opts.SourceContainer,
                    opts.SourceKey,
                    opts.SourceSAS,
                    opts.SourceFilePrefix,
                    opts.SourceEndpointSuffix,
                    opts.SortFilenames,
                    opts.SourceFiles.ToList(),
                    opts.DestAccountName,
                    opts.DestKey,
                    opts.DestSAS,
                    opts.DestContainer,
                    opts.DestFilename,
                    opts.DestEndpointSuffix,
                    opts.ColHeader,
                    opts.CalcMD5ForBlock,
                    opts.Overwrite,
                    opts.ServerTimeout,
                    opts.MaxDOP,
                    opts.UseRetry,
                    myLogger,
                    progress).GetAwaiter().GetResult() ? 0 : 1;
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
                    opts.DestSAS,
                    opts.DestContainer,
                    opts.DestFilename,
                    opts.DestEndpointSuffix,
                    opts.ColHeader,
                    opts.CalcMD5ForBlock,
                    opts.ServerTimeout,
                    opts.MaxDOP,
                    opts.UseRetry,
                    myLogger,
                    progress
                    ).GetAwaiter().GetResult() ? 0 : 1;
            },
            errs => 1);

            if (retVal != 0)
            {
                // allow some time for logs to flush
                System.Threading.Thread.Sleep(TimeSpan.FromSeconds(10));
            }

            return retVal;
        }
    }    
}

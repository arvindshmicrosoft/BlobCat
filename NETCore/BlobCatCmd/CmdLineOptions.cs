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
    using System;
    using System.Collections.Generic;

    [Verb("concatblob", HelpText = "Concatenates a set of blobs into a single blob.")]
    class ConcatBlobOptions
    {
        [Option("SourceEndpointSuffix", Required = false, HelpText = "The source storage endpoint suffix to use", Default = "core.windows.net")]
        public string SourceEndpointSuffix { get; set; }

        [Option("SourceAccount", Required = true, HelpText = "Source storage account name")]
        public string SourceAccountName { get; set; }

        [Option("SourceContainer", Required = true, HelpText = "Source container name")]
        public string SourceContainer { get; set; }

        [Option("SourceKey", Required = false, HelpText = "Source storage account key")]
        public string SourceKey { get; set; }

        [Option("SourceSAS", Required = false, HelpText = "Source SAS")]
        public string SourceSAS { get; set; }

        [Option("SourceFiles", Required = false, HelpText = "Source file name(s) - must include folder name(s) if applicable")]
        public IEnumerable<string> SourceFiles { get; set; }

        [Option("SourceFilePrefix", Required = false, HelpText = "Source file prefix - this can include folder hierarchy (if applicable)")]
        public string SourceFilePrefix { get; set; }

        [Option("SortFilenames", Required = false, Default = false, HelpText = "(true | false) Whether to sort the input file names")]
        public bool SortFilenames { get; set; }

        [Option("DestEndpointSuffix", Required = false, HelpText = "The Dest storage endpoint suffix to use", Default = "core.windows.net")]
        public string DestEndpointSuffix { get; set; }

        [Option("DestAccount", Required = true, HelpText = "Destination storage account name")]
        public string DestAccountName { get; set; }

        [Option("DestContainer", Required = true, HelpText = "Destination container name")]
        public string DestContainer { get; set; }

        [Option("DestKey", Required = false, HelpText = "Destination storage account key")]
        public string DestKey { get; set; }

        [Option("DestSAS", Required = false, HelpText = "Destination SAS")]
        public string DestSAS { get; set; }

        [Option("DestFilename", Required = true, HelpText = "Destination file name")]
        public string DestFilename { get; set; }

        [Option("ColHeader", Required = false, HelpText = "Column header string")]
        public string ColHeader { get; set; }

        [Option("FileSeparator", Required = false, HelpText = "File Separator string")]
        public string FileSeparator { get; set; }

        [Option("CalcMD5ForBlock", Default = false, Required = false, HelpText = "(true | false) Whether to sort the input file names")]
        public bool CalcMD5ForBlock { get; set; }

        [Option("Overwrite", Default = false, Required = false, HelpText = "(true | false) Overwrite the destination blob")]
        public bool Overwrite { get; set; }

        [Option("Debug", Default = false, Required = false, HelpText = "(true | false) Include debug output")]
        public bool DebugOutput { get; set; }

        [Option("ExecutionTimeout", Default = 120, Required = false, HelpText = "ExecutionTimeout in seconds")]
        public int ExecutionTimeout { get; set; }

        [Option("MaxDOP", Default = 8, Required = false, HelpText = "Number of parallel tasks to use for concatenation (default 8)")]
        public int MaxDOP { get; set; }

        [Option("UseInbuiltRetry", Default = true, Required = false, HelpText = "Set this to false to not use the Azure Storage inbuilt retry.")]
        public bool UseInbuiltRetry { get; set; }

        [Option("RetryCount", Default = 5, Required = false, HelpText = "How many times to retry blob operations")]
        public int RetryCount { get; set; }
    }

    [Verb("filestoblob", HelpText = "Concatenates a set of on-disk files into a single blob.")]
    class FilesToBlobOptions
    {
        [Option("SourceFolder", Required = true, HelpText ="Folder name where the source files are stored")]
        public string SourceFolder { get; set; }

        [Option("SourceFiles", Required = false, HelpText = "Source file name(s) - either this or the file name prefix must be specified")]
        public IEnumerable<string> SourceFiles { get; set; }

        [Option("SourceFilePrefix", Required = false, HelpText = "Source file name prefix")]
        public string SourceFilePrefix { get; set; }

        [Option("SortFilenames", Default = false, Required = false, HelpText = "(true | false) Whether to sort the input file names")]
        public bool SortFilenames { get; set; }

        [Option("DestEndpointSuffix", Required = false, HelpText = "The Dest storage endpoint suffix to use", Default = "core.windows.net")]
        public string DestEndpointSuffix { get; set; }

        [Option("DestAccount", Required = true, HelpText = "Destination storage account name")]
        public string DestAccountName { get; set; }

        [Option("DestContainer", Required = true, HelpText = "Destination container name")]
        public string DestContainer { get; set; }

        [Option("DestKey", Required = false, HelpText = "Destination storage account key")]
        public string DestKey { get; set; }

        [Option("DestSAS", Required = false, HelpText = "Destination SAS")]
        public string DestSAS { get; set; }

        [Option("DestFilename", Required = true, HelpText = "Destination file name")]
        public string DestFilename { get; set; }

        [Option("ColHeader", Required = false, HelpText = "Column header string")]
        public string ColHeader { get; set; }

        [Option("FileSeparator", Required = false, HelpText = "File Separator string")]
        public string FileSeparator { get; set; }

        [Option("CalcMD5ForBlock", Default = false, Required = false, HelpText = "(true | false) Whether to sort the input file names")]
        public bool CalcMD5ForBlock { get; set; }

        [Option("Overwrite", Default = false, Required = false, HelpText = "(true | false) Overwrite the destination blob")]
        public bool Overwrite { get; set; }

        [Option("Debug", Default = false, Required = false, HelpText = "(true | false) Include debug output")]
        public bool DebugOutput { get; set; }

        [Option("ExecutionTimeout", Default = 120, Required = false, HelpText = "ExecutionTimeout in seconds")]
        public int ExecutionTimeout { get; set; }

        [Option("MaxDOP", Default = 8, Required = false, HelpText = "Number of parallel tasks to use for concatenation (default 8)")]
        public int MaxDOP { get; set; }

        [Option("UseInbuiltRetry", Default = true, Required = false, HelpText = "Set this to false to not use the Azure Storage inbuilt retry.")]
        public bool UseInbuiltRetry { get; set; }

        [Option("RetryCount", Default = 5, Required = false, HelpText = "How many times to retry blob operations")]
        public int RetryCount { get; set; }
    }
}

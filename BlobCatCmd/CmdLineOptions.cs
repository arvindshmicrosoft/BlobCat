using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CommandLine;

namespace Microsoft.Azure.Samples.BlobCat
{
    [Verb("concatblob", HelpText = "Concatenates a set of blobs into a single blob.")]
    class ConcatBlobOptions
    {
        [Option("SourceAccount", Required = false, HelpText = "Source storage account name")]
        public string SourceAccountName { get; set; }

        [Option("SourceContainer", Required = false, HelpText = "Source container name")]
        public string SourceContainer { get; set; }

        [Option("SourceKey", Required = false, HelpText = "Source storage account key")]
        public string SourceKey { get; set; }

        [Option("SourceFiles", Required = false, HelpText = "Source file name(s) - must include folder name(s) if applicable")]
        public IEnumerable<string> SourceFiles { get; set; }

        [Option("SourceFilePrefix", Required = false, HelpText = "Source file prefix - this can include folder hierarchy (if applicable)")]
        public string SourceFilePrefix { get; set; }

        [Option("SortFilenames", Required = false, HelpText = "(true | false) Whether to sort the input file names")]
        public bool SortFilenames { get; set; }

        [Option("DestAccount", Required = false, HelpText = "Destination storage account name")]
        public string DestAccountName { get; set; }

        [Option("DestContainer", Required = false, HelpText = "Destination container name")]
        public string DestContainer { get; set; }

        [Option("DestKey", Required = false, HelpText = "Destination storage account key")]
        public string DestKey { get; set; }

        [Option("DestFilename", Required = false, HelpText = "Destination file name")]
        public string DestFilename { get; set; }
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

        [Option("SortFilenames", Required = false, HelpText = "(true | false) Whether to sort the input file names")]
        public bool SortFilenames { get; set; }

        [Option("DestAccount", Required = false, HelpText = "Destination storage account name")]
        public string DestAccountName { get; set; }

        [Option("DestContainer", Required = false, HelpText = "Destination container name")]
        public string DestContainer { get; set; }

        [Option("DestKey", Required = false, HelpText = "Destination storage account key")]
        public string DestKey { get; set; }
        
        [Option("DestFilename", Required = false, HelpText = "Destination file name")]
        public string DestFilename { get; set; }
    }
}

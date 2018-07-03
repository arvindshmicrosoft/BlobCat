# BlobCat 
Concatenates blobs in Azure Storage. Source can be other block blobs or files on disk. Big Data platforms and SQL Polybase export files in a distributed fashion, which means a file called part.txt will actually be multiple files like part_1.txt, part_2.txt etc. The user then typically has to download and then concatenate them somehow. This utility is aimed at making that simple by offering a blob-to-blob or even local-disk-to-blob concatenation (why the latter? In case you have on-prem Hadoop or SQL Server with Polybase and then want to concat files directly into Azure storage).
 
# Prerequisites
The utility is built on .NET Core, which means it runs on Windows, Mac and Linux (I have tested with Windows 10, Windows 2016 and Ubuntu 16.04). You will need .NET Core 2.1 runtime to run this utility. .NET Core runtime is a small download, around 30 MB max. [The .NET download page](https://www.microsoft.com/net/download) has all the links, again remember you just need the RUNTIME not the whole SDK to execute this utility. Scroll down on that page to get to the section where the runtimes are listed in the second column of that table.

# Usage 
Use command line execution

	dotnet blobcat.dll <verb> <options>

\<verb\> is currently either of the following:

* *concatblob*: Concatenates a set of blobs into a single blob.
* *filestoblob*: Concatenates a set of on-disk files into a single blob.

\<options\> vary for each verb, see sections below for details.

## Concatenate blob-to-blob syntax
A sample command line (for the blob-to-blob scenario) is shown below. it concatenates any files beginning with the specified prefix in the specified Azure storage account and container, into the destination Azure storage account and container.
	
	dotnet blobcat.dll concatblob --SourceAccount <source Azure storage account name> --SourceContainer <source Azure storage container name> --SourceKey <source Azure storage account key> | --SourceSAS <source SAS> --SourceFilePrefix <prefix> --SortFilenames true --DestAccount <Azure storage account name> --DestContainer <somecontainer> --DestFilename <somefilename can include foldername> --DestKey <Storage Key> | --DestSAS <dest SAS> --ColHeader <string representing the column headers>

For a short explanation of each of these switches, you can run:
	
	dotnet blobcat.dll concatblob --help

## Concatenate files-to-blob syntax
A sample command line (for the files-to-blob scenario) is shown below. it concatenates any files (on the local disk) beginning with the specified prefix into the destination Azure storage account and container.

	dotnet blobcat.dll filestoblob --SourceFolder <folder path> --SourceFilePrefix <prefix> --SortFilenames true --DestAccount <Azure storage account name> --DestContainer <somecontainer> --DestFilename <somefilename can include foldername> --DestKey <Storage Key> | --DestSAS <dest SAS> --ColHeader <string representing the column headers>

For a short explanation of each of these switches, you can run:

	dotnet blobcat.dll filestoblob --help

# Acknowledgements
Thank you to the [CommandLineParser](https://github.com/commandlineparser/commandline) team!

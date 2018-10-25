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
    using Microsoft.Extensions.Logging;
    using WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    public class BlobCatEngine
    {
        // currently 100 MB
        private static readonly int CHUNK_SIZE = 100 * 1024 * 1024;

        private static void GlobalOptimizations()
        {
            // First two are Best practices optimizations for Blob, as per the Azure Storage GitHub these are highly recommended.
            // The Threadpool setting - I had just played with it. 
            // TODO review this if it still applies to .NET Core
            ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 8;
            ServicePointManager.Expect100Continue = false;

            // TODO Check if this is also still required
            ThreadPool.SetMinThreads(Environment.ProcessorCount * 25, Environment.ProcessorCount * 8);
        }

        /// <summary>
        /// Concatenate a set of blobs - specified either via their prefix or an explicit list of blob names - to a single destination blob
        /// Source blobs must be from a single container. However because of the way the Azure Storage Block Blob works, you can call
        /// this function multiple times, specifying different sets of source blobs each time, and they will simply be "appended" to the destination blob.
        /// </summary>
        /// <param name="sourceStorageAccountName"></param>
        /// <param name="sourceStorageContainerName"></param>
        /// <param name="sourceStorageAccountKey"></param>
        /// <param name="isSourceSAS"></param>
        /// <param name="sourceBlobPrefix"></param>
        /// <param name="sourceEndpointSuffix"></param>
        /// <param name="sortBlobs"></param>
        /// <param name="sourceSAS"></param>
        /// <param name="sourceBlobNames"></param>
        /// <param name="destEndpointSuffix"></param>
        /// <param name="destSAS"></param>
        /// <param name="destStorageAccountName"></param>
        /// <param name="destStorageAccountKey"></param>
        /// <param name="isDestSAS"></param>
        /// <param name="destStorageContainerName"></param>
        /// <param name="destBlobName"></param>
        /// <param name="colHeader"></param>
        /// <param name="calcMD5ForBlocks"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public async static Task<bool> BlobToBlob(string sourceStorageAccountName,
            string sourceStorageContainerName,
            string sourceStorageAccountKey,
            string sourceSAS,
            string sourceBlobPrefix,
            string sourceEndpointSuffix,
            bool sortBlobs,
            List<string> sourceBlobNames,
            string destStorageAccountName,
            string destStorageAccountKey,
            string destSAS,
            string destStorageContainerName,
            string destBlobName,
            string destEndpointSuffix,
            string colHeader,
            string fileSeparator,
            bool calcMD5ForBlock,
            bool overwriteDest,
            int timeoutSeconds,
            int maxDOP,
            bool useInbuiltRetry,
            int retryCount,
            ILogger logger,
            IProgress<OpProgress> progress)
        {
            var opProgress = new OpProgress();

            try
            {
                var sw = Stopwatch.StartNew();

                GlobalOptimizations();

                var res = await BlobHelpers.GetDestinationBlobBlockList(destStorageAccountName,
                    destStorageContainerName,
                    destBlobName,
                    destSAS,
                    destStorageAccountKey,
                    destEndpointSuffix,
                    overwriteDest,
                    retryCount,
                    logger);

                var destBlockList = res.blockList;
                var destBlob = res.destBlob;

                if (destBlockList is null)
                {
                    return false;
                }

                // create a place holder for the final block list (to be eventually used for put block list) and pre-populate it with the known list of blocks
                // already associated with the destination blob
                var finalBlockList = new List<string>(destBlockList);

                // signal back to the caller with the total block list

                // check if there is a specific list of blobs given by the user, in which case the immediate 'if' code below will be skipped
                if (sourceBlobNames is null || sourceBlobNames.Count == 0)
                {
                    // now just get the blob names, that's all we need for further processing
                    sourceBlobNames = await BlobHelpers.GetBlobListing(sourceStorageAccountName,
                        sourceStorageContainerName,
                        sourceBlobPrefix,
                        sourceSAS,
                        sourceStorageAccountKey,
                        sourceEndpointSuffix,
                        retryCount,
                        logger);

                    // check for null being returned from above in which case we need to exit
                    if (sourceBlobNames is null)
                    {
                        logger.LogError($"Souce blob listing failed to return any results. Exiting!");
                        return false;
                    }

                    // if the user specified to sort the input blobs (only valid for the prefix case) then we will happily do that!
                    // The gotcha here is that this is a string sort. So if blobs have names like blob_9, blob_13, blob_6, blob_3, blob_1
                    // the sort order will result in blob_1, blob_13, blob_3, blob_6, blob_9. 
                    // To avoid issues like this the user must 0-prefix the numbers embedded in the filenames.
                    if (sortBlobs)
                    {
                        sourceBlobNames.Sort();
                    }
                }

                // var sourceBlobItems = new List<BlobItem>();
                var sourceBlockList = new Dictionary<string, List<BlockRangeBase>>();

                // we first need to seed with the column header if it was specified
                PrefixColumnHeaderIfApplicable(sourceBlockList,
                    colHeader,
                    sourceEndpointSuffix,
                    sourceStorageAccountName,
                    sourceStorageContainerName);

                // it is necessary to use a dictionary to hold the BlobItem because we do want to preserve sort order if necessary
                int tmpBlobIndex = 0;
                foreach (var srcBlob in sourceBlobNames)
                {
                    sourceBlockList.Add(srcBlob, new List<BlockRangeBase>());

                    InjectFileSeparator(sourceBlockList,
                        fileSeparator,
                        tmpBlobIndex,
                        sourceEndpointSuffix,
                        sourceStorageAccountName,
                        sourceStorageContainerName);

                    tmpBlobIndex++;
                }

                // sourceBlobItems.AddRange(sourceBlobNames.Select(b => new BlobItem { sourceBlobName = b }));

                // get block lists for all source blobs in parallel. earlier this was done serially and was found to be bottleneck
                Parallel.ForEach(sourceBlobNames, srcBlobName =>
                {
                    // var tmpBlobItem = new BlobItem();

                    var tmpSrcBlob = BlobHelpers.GetBlockBlob(sourceStorageAccountName,
                        sourceStorageContainerName,
                        srcBlobName,
                        sourceSAS,
                        sourceStorageAccountKey,
                        sourceEndpointSuffix,
                        retryCount,
                        logger);

                    if (tmpSrcBlob is null)
                    {
                        // throw exception. this condition will only be entered if a blob name was explicitly specified and it does not really exist
                        throw new StorageException($"An invalid source blob ({srcBlobName}) was specified.");
                    }

                    // first we get the block list of the source blob. we use this to later parallelize the download / copy operation
                    var tmpBlockList = BlobHelpers.GetBlockListForBlob(tmpSrcBlob, retryCount, logger).GetAwaiter().GetResult();

                    // proceed to construct a List<BlobBlockRange> to add to the master dictionary

                    // iterate through the list of blocks and compute their effective offsets in the final file.
                    long currentOffset = 0;
                    int chunkIndex = 0;
                    var blocksToBeAdded = new List<BlobBlockRange>();

                    if (tmpBlockList.Count() == 0 && tmpSrcBlob.Properties.Length > 0)
                    {
                        // in case the source blob is smaller then 256 MB (for latest API) then the blob is stored directly without any block list
                        // so in this case the sourceBlockList is 0-length and we need to fake a BlockListItem as null, which will later be handled below
                        var blockLength = tmpSrcBlob.Properties.Length;

                        blocksToBeAdded.Add(new BlobBlockRange(tmpSrcBlob,
                                                    string.Concat(
                                                        sourceEndpointSuffix,
                                                        sourceStorageAccountName,
                                                        sourceStorageContainerName,
                                                        srcBlobName,
                                                        blockLength,
                                                        chunkIndex),
                                                    currentOffset,
                                                    blockLength));
                    }
                    else
                    {
                        foreach (var blockListItem in tmpBlockList)
                        {
                            var blockLength = blockListItem.Length;

                            // compute a unique blockId based on blob account + container + blob name (includes path) + block length + block "number"
                            // TODO also consider a fileIndex component, to eventually allow for the same source blob to recur in the list of source blobs
                            blocksToBeAdded.Add(new BlobBlockRange(tmpSrcBlob,
                                string.Concat(
                                    sourceEndpointSuffix,
                                    sourceStorageAccountName,
                                    sourceStorageContainerName,
                                    srcBlobName,
                                    blockLength,
                                    chunkIndex),
                                currentOffset,
                                blockLength));

                            // increment this here itself as we may potentially skip to the next blob
                            chunkIndex++;
                            currentOffset += blockLength;
                        }
                    }

                    lock (sourceBlockList)
                    {
                        sourceBlockList[srcBlobName].AddRange(blocksToBeAdded);                        
                    }
                });

                // the total number of "ticks" to be reported will be the number of blocks + the number of blobs
                // this is because each PutBlock is reported separately, as is the PutBlockList when each source blob is finished
                opProgress.TotalTicks = sourceBlockList.Count + sourceBlockList.Values.Select(b => b.Count()).Sum();

                progress.Report(opProgress);

                if (!await BlockRangeWorkers.ProcessSourceBlocks(sourceBlockList,
                    destBlob,
                    destBlockList,
                    finalBlockList,
                    calcMD5ForBlock,
                    timeoutSeconds,
                    maxDOP,
                    useInbuiltRetry,
                    retryCount,
                    opProgress,
                    progress,
                    logger))
                {
                    return false;
                }

                sw.Stop();

                opProgress.StatusMessage = $"BlobToBlob operation suceeded in {sw.Elapsed.TotalSeconds} seconds.";
                progress.Report(opProgress);
            }
            catch (StorageException ex)
            {
                //opProgress.Percent = 100;
                //opProgress.StatusMessage = "Errors occured. Details in the log.";
                //progress.Report(opProgress);

                BlobHelpers.LogStorageException("Unhandled exception in BlobToBlob", ex, logger, false);

                return false;
            }

            return true;
        }

        private static void InjectFileSeparator(Dictionary<string, List<BlockRangeBase>> sourceBlockList,
            string fileSeparator,
            int fileIndex,
            string sourceEndpointSuffix,
            string sourceStorageAccountName,
            string sourceStorageContainerName)
        {
            // if the user specified a file separator string to be appended after each file, 
            // append that now as a string block range
            if (!string.IsNullOrEmpty(fileSeparator))
            {
                var fileSepKey = string.Concat(fileSeparator, fileIndex);

                sourceBlockList.Add(fileSepKey, new List<BlockRangeBase>());

                sourceBlockList[fileSepKey].Add(new StringBlockRange(Regex.Unescape(fileSeparator),
                        string.Concat(
                            fileSeparator,
                            fileIndex,
                            sourceEndpointSuffix,
                            sourceStorageAccountName,
                            sourceStorageContainerName)));
            }
        }

        private static void PrefixColumnHeaderIfApplicable(Dictionary<string, List<BlockRangeBase>> sourceBlockList,
            string colHeader,
            string sourceEndpointSuffix,
            string sourceStorageAccountName,
            string sourceStorageContainerName)
        {
            // check if we have a column header string specified, if so, prepend it to the list of "source blobs" 
            // clearly denoting this has to be used as a string
            if (!string.IsNullOrEmpty(colHeader))
            {
                sourceBlockList.Add(colHeader, new List<BlockRangeBase>());

                sourceBlockList[colHeader].Add(new StringBlockRange(Regex.Unescape(colHeader),
                        string.Concat(
                            sourceEndpointSuffix,
                            sourceStorageAccountName,
                            sourceStorageContainerName,
                            colHeader)));
            }
        }

        /// <summary>
        /// Concatenates a set of files on disk into a single block blob in Azure Storage. 
        /// The destination block blob can exist; in which case the files will be appended to the existing blob.
        /// </summary>
        /// <param name="sourceFolderName"></param>
        /// <param name="sourceBlobPrefix"></param>
        /// <param name="sortFiles"></param>
        /// <param name="sourceFileNames"></param>
        /// <param name="destStorageAccountName"></param>
        /// <param name="destStorageAccountKey"></param>
        /// <param name="destSAS"></param>
        /// <param name="destStorageContainerName"></param>
        /// <param name="destBlobName"></param>
        /// <param name="destEndpointSuffix"></param>
        /// <param name="colHeader"></param>
        /// <param name="calcMD5ForBlocks"></param>
        /// <param name="logger"></param>
        /// <returns>
        /// True if successful; False if errors found
        /// </returns>
        public static async Task<bool> DiskToBlob(string sourceFolderName,
            string sourceBlobPrefix,
            bool sortFiles,
            List<string> sourceFileNames,
            string destStorageAccountName,
            string destStorageAccountKey,
            string destSAS,
            string destStorageContainerName,
            string destBlobName,
            string destEndpointSuffix,
            string colHeader,
            string fileSeparator,
            bool calcMD5ForBlock,
            bool overwriteDest,
            int timeoutSeconds,
            int maxDOP,
            bool useInbuiltRetry,
            int retryCount,
            ILogger logger,
            IProgress<OpProgress> progress
            )
        {
            var opProgress = new OpProgress();

            try
            {
                var sw = Stopwatch.StartNew();

                GlobalOptimizations();

                var res = await BlobHelpers.GetDestinationBlobBlockList(destStorageAccountName,
                    destStorageContainerName,
                    destBlobName,
                    destSAS,
                    destStorageAccountKey,
                    destEndpointSuffix,
                    overwriteDest,
                    retryCount,
                    logger);

                var destBlockList = res.blockList;
                var destBlob = res.destBlob;

                if (destBlockList is null)
                {
                    return false;
                }

                // this will start off as blank; we will keep appending to this the list of block IDs for each file
                var finalBlockList = new List<string>();

                // check if there is a specific list of files given by the user, in which case the immediate 'if' code below will be skipped
                if (sourceFileNames is null || sourceFileNames.Count == 0)
                {
                    // we have a prefix specified, so get a of blobs with a specific prefix and then add them to a list
                    sourceFileNames = Directory.GetFiles(sourceFolderName, sourceBlobPrefix + "*").ToList();

                    // if the user specified to sort the input blobs (only valid for the prefix case) then we will happily do that!
                    // The gotcha here is that this is a string sort. So if files have names like file_9, file_13, file_6, file_3, file_1
                    // the sort order will result in file_1, file_13, file_3, file_6, file_9. 
                    // To avoid issues like this the user must 0-prefix the numbers embedded in the filenames.
                    if (sortFiles)
                    {
                        sourceFileNames.Sort();
                    }
                }

                var sourceBlockList = new Dictionary<string, List<BlockRangeBase>>();

                // we first need to seed with the column header if it was specified
                PrefixColumnHeaderIfApplicable(sourceBlockList,
                    colHeader,
                    null,
                    null,
                    null);

                int tmpBlobIndex = 0;

                // it is necessary to use a dictionary to hold the BlobItem because we do want to preserve sort order if necessary
                foreach (var srcBlob in sourceFileNames)
                {
                    sourceBlockList.Add(srcBlob, new List<BlockRangeBase>());

                    InjectFileSeparator(sourceBlockList,
                        fileSeparator,
                        tmpBlobIndex,
                        null,
                        null,
                        null);

                    tmpBlobIndex++;
                }

                foreach (var sourceFileName in sourceFileNames)
                {
                    var fInfo = new FileInfo(sourceFileName);

                    // construct the set of "block ranges" by splitting the source files into chunks
                    // of CHUNK_SIZE or the remaining length, as appropriate.
                    var chunkSet = Enumerable.Range(0, (int)(fInfo.Length / CHUNK_SIZE) + 1)
                        .Select(ci => new FileBlockRange(sourceFileName,
                        string.Concat(sourceFileName,
                            fInfo.Length,
                            ci),
                        ci * CHUNK_SIZE,
                        (ci * CHUNK_SIZE + CHUNK_SIZE > fInfo.Length) ?
                                fInfo.Length - ci * CHUNK_SIZE :
                                CHUNK_SIZE
                        ));

                    sourceBlockList[sourceFileName].AddRange(chunkSet);
                }

                // the total number of "ticks" to be reported will be the number of blocks + the number of blobs
                // this is because each PutBlock is reported separately, as is the PutBlockList when each source blob is finished
                opProgress.TotalTicks = sourceBlockList.Count + sourceBlockList.Values.Select(b => b.Count()).Sum();

                progress.Report(opProgress);

                if (!await BlockRangeWorkers.ProcessSourceBlocks(sourceBlockList,
                    destBlob,
                    destBlockList,
                    finalBlockList,
                    calcMD5ForBlock,
                    timeoutSeconds,
                    maxDOP,
                    useInbuiltRetry,
                    retryCount,
                    opProgress,
                    progress,
                    logger))
                {
                    return false;
                }

                sw.Stop();

                opProgress.StatusMessage = $"DiskToBlob operation suceeded in {sw.Elapsed.TotalSeconds} seconds.";
            }
            catch (StorageException ex)
            {
                opProgress.Percent = 100;
                opProgress.StatusMessage = "Errors occured. Details in the log.";
                progress.Report(opProgress);

                BlobHelpers.LogStorageException("Unhandled exception in DiskToBlob", ex, logger, false);

                return false;
            }

            return true;
        }
    }
}

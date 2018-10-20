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

        private static double overallProgress = 0.0;

        private static Mutex mutexForProgress = new Mutex(false);

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
            bool calcMD5ForBlock,
            bool overwriteDest,
            int timeoutSeconds,
            int maxDOP,
            bool useRetry,
            ILogger logger,
            IProgress<OpProgress> progress)
        {
            var opProgress = new OpProgress();

            try
            {
                var sw = Stopwatch.StartNew();

                GlobalOptimizations();

                var destBlob = BlobHelpers.GetBlockBlob(destStorageAccountName,
                    destStorageContainerName,
                    destBlobName,
                    destSAS,
                    destStorageAccountKey,
                    destEndpointSuffix,
                    logger);

                if (destBlob is null)
                {
                    logger.LogError($"Failed to get a reference to destination conatiner / blob {destBlobName}; exiting!");
                    return false;
                }

                var destBlockList = new List<string>();

                // check if the blob exists, in which case we need to also get the list of blocks associated with that blob
                // this will help to skip blocks which were already completed, and thereby help with resume
                // TODO Block IDs are not unique across files - this will trip up the logic
                // only get committed blocks to be sure 
                var blockList = await BlobHelpers.GetBlockListForBlob(destBlob, logger);
                if (blockList is null)
                {
                    // this is when the destination blob does not yet exist
                    logger.LogDebug($"Destination blob {destBlobName} does not exist (block listing returned null).");
                }
                else
                {
                    // support overwrite by deleting the destination blob
                    if (overwriteDest)
                    {
                        logger.LogDebug($"Destination blob {destBlobName} exists but needs to be deleted as overwrite == true.");

                        await destBlob.DeleteAsync();

                        logger.LogDebug($"Destination blob {destBlobName} deleted to prepare for overwrite.");
                    }
                    else
                    {
                        logger.LogDebug($"Destination blob {destBlobName} exists; trying to get block listing.");

                        destBlockList = new List<string>(blockList.Select(b => b.Name));

                        logger.LogDebug($"Destination blob {destBlobName} has {destBlockList.Count} blocks.");
                    }
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

                var sourceBlobItems = new List<BlobItem>();

                // first, do we have a column header string specified, if so, prepend it to the list of "source blobs" 
                // clearly denoting this has to be used as a string
                if (!string.IsNullOrEmpty(colHeader))
                {
                    sourceBlobItems.Add(new BlobItem()
                    {
                        sourceBlobName = colHeader,
                        useAsString = true
                    });
                }

                // now copy in the rest of the "regular" blobs
                sourceBlobItems.AddRange(from b in sourceBlobNames select new BlobItem { sourceBlobName = b });

                // get block lists for all source blobs in parallel. earlier this was done serially and was found to be bottleneck
                Parallel.ForEach(sourceBlobItems, tmpBlobItem =>
                {
                    tmpBlobItem.blockBlob = tmpBlobItem.useAsString ? null : BlobHelpers.GetBlockBlob(sourceStorageAccountName,
                        sourceStorageContainerName,
                        tmpBlobItem.sourceBlobName,
                        sourceSAS,
                        sourceStorageAccountKey,
                        sourceEndpointSuffix,
                        logger);

                    // first we get the block list of the source blob. we use this to later parallelize the download / copy operation
                    tmpBlobItem.blockList = BlobHelpers.GetBlockListForBlob(tmpBlobItem.blockBlob, logger).GetAwaiter().GetResult();
                });

                opProgress.TotalTicks = (from b in sourceBlobItems select b.blockList.Count()).Sum()
                    + sourceBlobItems.Count;

                progress.Report(opProgress);

                // iterate through each source blob, one at a time.
                int sourceBlobIndex = 0;

                foreach (var currBlobItem in sourceBlobItems)
                {
                    var blockRanges = new List<BlockRangeBase>();
                    long currentOffset = 0;

                    var sourceBlobName = currBlobItem.sourceBlobName;

                    if (!currBlobItem.useAsString && currBlobItem.blockBlob is null)
                    {
                        logger.LogError($"Failed to get reference to source blob {sourceBlobName}, exiting!");
                        return false;
                    }

                    if (currBlobItem.useAsString)
                    {
                        var newBlockRange = new StringBlockRange(Regex.Unescape(currBlobItem.sourceBlobName),
                            string.Concat(
                                sourceEndpointSuffix,
                                sourceStorageAccountName,
                                sourceStorageContainerName,
                                currBlobItem.sourceBlobName));

                        // check if this block has already been copied + committed at the destination, and in that case, skip it
                        if (!destBlockList.Contains(newBlockRange.Name))
                        {
                            blockRanges.Add(newBlockRange);
                        }
                    }
                    else
                    {
                        opProgress.StatusMessage = $"Working on {sourceBlobName}";

                        logger.LogDebug($"START: {sourceBlobName}");

                        if (currBlobItem.blockList is null)
                        {
                            logger.LogError($"Failed to get block list for source blob {sourceBlobName}; exiting!");
                            return false;
                        }

                        // in case the source blob is smaller then 256 MB (for latest API) then the blob is stored directly without any block list
                        // so in this case the sourceBlockList is 0-length and we need to fake a BlockListItem as null, which will later be handled below
                        if (currBlobItem.blockList.Count() == 0 && currBlobItem.blockBlob.Properties.Length > 0)
                        {
                            logger.LogDebug($"Source blob {sourceBlobName} does not have blocks as it is a 'small blob'");

                            ListBlockItem fakeBlockItem = null;
                            currBlobItem.blockList = currBlobItem.blockList.Concat(new[] { fakeBlockItem });
                        }

                        // iterate through the list of blocks and compute their effective offsets in the final file.
                        int chunkIndex = 0;
                        foreach (var blockListItem in currBlobItem.blockList)
                        {
                            if (blockListItem != null)
                            {
                                logger.LogDebug($"Processing blockID {blockListItem.Name} for source blob {sourceBlobName}");
                            }

                            // handle special case when the sourceBlob was put using PutBlob and has no blocks
                            var blockLength = blockListItem == null ? currBlobItem.blockBlob.Properties.Length : blockListItem.Length;

                            // compute a unique blockId based on blob account + container + blob name (includes path) + block length + block "number"
                            // TODO also consider a fileIndex component, to eventually allow for the same source blob to recur in the list of source blobs

                            var newBlockRange = new BlobBlockRange(currBlobItem.blockBlob,
                                string.Concat(
                                    sourceEndpointSuffix,
                                    sourceStorageAccountName,
                                    sourceStorageContainerName,
                                    sourceBlobName,
                                    blockLength,
                                    chunkIndex),
                                currentOffset,
                                blockLength
                                );

                            // increment this here itself as we may potentially skip to the next blob
                            chunkIndex++;
                            currentOffset += blockLength;

                            // check if this block has already been copied + committed at the destination, and in that case, skip it
                            if (destBlockList.Contains(newBlockRange.Name))
                            {
                                logger.LogDebug($"Destination already has blockID {newBlockRange.Name} for source blob {sourceBlobName}; skipping");

                                continue;
                            }
                            else
                            {
                                logger.LogDebug($"Adding blockID {newBlockRange.Name} for source blob {sourceBlobName} to work list");

                                blockRanges.Add(newBlockRange);
                            }
                        }
                    }

                    logger.LogDebug($"Total number of of ranges to process for source blob {sourceBlobName} is {blockRanges.Count}");

                    // add this list of block IDs to the final list
                    finalBlockList.AddRange(blockRanges.Select(e => e.Name));

                    // reset back to 0 to actually execute the copies
                    currentOffset = 0;

                    // call the helper function to actually execute the writes to blob
                    await ProcessBlockRanges(blockRanges,
                        destBlob,
                        calcMD5ForBlock,
                        logger,
                        progress,
                        opProgress,
                        (double)sourceBlobIndex / (double)sourceBlobItems.Count * 100.0,
                        1.0 / (double)sourceBlobItems.Count * 100.0,
                        timeoutSeconds,
                        maxDOP,
                        useRetry);

                    // each iteration (each source file) we will commit an ever-increasing super-set of block IDs
                    // we do this to support "resume" operations later on.
                    // we will only do this if we actually did any work here TODO review
                    if (blockRanges.Count() > 0 && sourceBlobIndex >= sourceBlobItems.Count - 1)
                    {
                        // use retry policy which will automatically handle the throttling related StorageExceptions
                        await BlobHelpers.GetStorageRetryPolicy(logger).ExecuteAsync(async () =>
                        {
                            await destBlob.PutBlockListAsync(finalBlockList);
                        });
                    }

                    logger.LogDebug($"END: {currBlobItem.sourceBlobName}");

                    // report progress to caller
                    sourceBlobIndex++;

                    opProgress.Percent = overallProgress;
                    opProgress.StatusMessage = $"Finished with {currBlobItem.sourceBlobName}";

                    progress.Report(opProgress);

                    // TODO optionally allow user to specify extra character(s) to append in between files. This is typically needed when the source files do not have a trailing \n character.
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

                BlobHelpers.LogStorageException(ex, logger, false);

                return false;
            }

            return true;
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
            bool calcMD5ForBlock,
            int timeoutSeconds,
            int maxDOP,
            bool useRetry,
            ILogger logger,
            IProgress<OpProgress> progress
            )
        {
            var opProgress = new OpProgress();

            try
            {
                var sw = Stopwatch.StartNew();

                GlobalOptimizations();

                // TODO check for null from below func
                // get a reference to the destination blob
                var destBlob = BlobHelpers.GetBlockBlob(destStorageAccountName,
                    destStorageContainerName,
                    destBlobName,
                    destSAS,
                    destStorageAccountKey,
                    destEndpointSuffix,
                    logger);

                var destBlockList = new List<string>();

                // this will start off as blank; we will keep appending to this the list of block IDs for each file
                var finalBlockList = new List<string>();

                // check if the blob exists, in which case we need to also get the list of blocks associated with that blob
                // this will help to skip blocks which were already completed, and thereby help with resume
                // TODO Block IDs are not unique across files - this will trip up the logic
                //if (await destBlob.ExistsAsync())
                //{
                //    // only get committed blocks to be sure 
                //    var blockList = await destBlob.DownloadBlockListAsync(BlockListingFilter.Committed, null, null, null);
                //    destBlockList = blockList.Select(e => e.Name).ToList();
                //}
                var blockList = await BlobHelpers.GetBlockListForBlob(destBlob, logger);
                if (blockList != null)
                {
                    destBlockList = blockList.Select(b => b.Name).ToList();
                }
                else
                {
                    // TODO this is an error condition, log and exit
                }

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

                // iterate through each source file, one at a time.
                foreach (var sourceFileName in sourceFileNames)
                {
                    logger.LogDebug("Starting on {sourceFileName}", sourceFileName);

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

                    // add this list of block IDs to the final list
                    finalBlockList.AddRange(chunkSet.Select(e => e.Name));

                    // chunkSet is the complete set of BlockRanges which should finally exist
                    // in the destination. However, we need to "subtract" the list of blocks already existing
                    // at the destination (depicted by destBlockList).
                    var blockRangesToBeCopied = chunkSet.Where(e => !destBlockList.Contains(e.Name));

                    // call the helper function to actually execute the writes to blob
                    // TODO this will be broken right now
                    await ProcessBlockRanges(blockRangesToBeCopied,
                        destBlob,
                        calcMD5ForBlock,
                        logger,
                        null,
                        null,
                        0,
                        0,
                        timeoutSeconds,
                        maxDOP,
                        useRetry);

                    // each iteration (each source file) we will commit an ever-increasing super-set of block IDs
                    // we do this to support "resume" operations later on.
                    // we will only do this if we actually did any work here TODO review
                    if (blockRangesToBeCopied.Count() > 0)
                    {
                        // use retry policy which will automatically handle the throttling related StorageExceptions
                        await BlobHelpers.GetStorageRetryPolicy(logger).ExecuteAsync(async () =>
                        {
                            await destBlob.PutBlockListAsync(finalBlockList);
                        });
                    }

                    logger.LogDebug($"{DateTime.Now}: END {sourceFileName}");

                    // TODO optionally allow user to specify extra character(s) to append in between files. This is typically needed when the source files do not have a trailing \n character.
                }

                sw.Stop();

                logger.LogInformation($"filestoblob operation suceeded in {sw.Elapsed.TotalSeconds} seconds.");
            }
            catch (StorageException ex)
            {
                opProgress.Percent = 100;
                opProgress.StatusMessage = "Errors occured. Details in the log.";
                progress.Report(opProgress);

                BlobHelpers.LogStorageException(ex, logger, false);

                return false;
            }

            return true;
        }

        private async static Task<bool> ProcessBlockRanges(IEnumerable<BlockRangeBase> blockRangesToBeCopied,
            CloudBlockBlob destBlob,
            bool calcMD5ForBlock,
            ILogger logger,
            IProgress<OpProgress> progress,
            OpProgress progressDetails,
            double baseProgress,
            double maxProgressPercent,
            int timeoutSeconds,
            int maxDOP,
            bool useRetry)
        {
            // int blockRangesDone = 0;

            var actionBlock = new ActionBlock<BlockRangeBase>(
            async (br) =>
            {
                var sw = Stopwatch.StartNew();
                //try
                {
                    await ProcessBlockRange(br, destBlob, timeoutSeconds, useRetry, calcMD5ForBlock, logger);

                    // Interlocked.Increment(ref blockRangesDone);

                    // here we increment the progress and update it
                    //mutexForProgress.WaitOne();

                    // overallProgress = baseProgress + (double)blockRangesDone / (double)blockRangesToBeCopied.Count() * maxProgressPercent;
                    // progressDetails.Percent = overallProgress;

                    //mutexForProgress.ReleaseMutex();

                    progress.Report(progressDetails);
                }
                // TODO should not be catching all exceptions
                //catch (Exception ex)
                //{
                //    logger.LogError(ex, "Could not process block range");
                //}

                sw.Stop();

                // TODO fix the logging to be fully relevant
                logger.LogDebug("{eventType} {duration} {context}", "ProcessBlockRanges", sw.Elapsed, $"ProcessBlockRange {br.Name} ended.");
            },
            new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = maxDOP // Environment.ProcessorCount * 8
            });

            foreach (var br in blockRangesToBeCopied)
            {
                await actionBlock.SendAsync(br);

                logger.LogDebug($"Sent block {br.Name} to the dataflow action block");

                // blockRangesDone++;
            }

            logger.LogDebug($"About to signal dataflow action block");

            actionBlock.Complete();

            logger.LogDebug($"Signalled dataflow action block");

            await actionBlock.Completion;

            logger.LogDebug($"Dataflow action block completed.");

            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceFileName"></param>
        /// <param name="currRange"></param>
        /// <param name="destBlob"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        private async static Task<bool> ProcessBlockRange(
            BlockRangeBase currRange, 
            CloudBlockBlob destBlob,
            int timeoutSeconds,
            bool useRetry,
            bool calcMD5ForBlock,
            ILogger logger)
        {
            //logger.LogInformation("{fileName} {segmentOffset} {action}",
            //    sourceFileName, currRange.StartOffset, "START");

            using (var brData = await currRange.GetBlockRangeData(calcMD5ForBlock, timeoutSeconds, logger))
            {
                logger.LogDebug($"Putting block {currRange.Name}");

                // use retry policy which will automatically handle the throttling related StorageExceptions
                await BlobHelpers.GetStorageRetryPolicy(logger).ExecuteAsync(async () =>
                    {
                        var blobReqOpts = new BlobRequestOptions()
                        {
                            ServerTimeout = new TimeSpan(0, 0, timeoutSeconds)
                        };

                        if (!useRetry) blobReqOpts.RetryPolicy = new WindowsAzure.Storage.RetryPolicies.NoRetry();

                        // reset the memory stream again to 0 and then call Azure Storage to put this as a block with the given block ID and MD5 hash
                        await destBlob.PutBlockAsync(currRange.Name, brData.MemStream, brData.Base64EncodedMD5Checksum,
                                        null, blobReqOpts, null);
                    });
            }

            return true;
        }
    }
}

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
    using Microsoft.WindowsAzure.Storage.Blob;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class BlockRangeWorkers
    {
        internal static async Task<bool> ProcessSourceBlocks(Dictionary<string, List<BlockRangeBase>> sourceBlockList,
            CloudBlockBlob destBlob,
            List<string> destBlockList,
            List<string> finalBlockList,
            bool calcMD5ForBlock,
            int timeoutSeconds,
            int maxDOP,
            bool useInbuiltRetry,
            int retryCount,
            OpProgress opProgress,
            IProgress<OpProgress> progress,
            ILogger logger)
        {
            foreach (var currBlobItem in sourceBlockList.Keys)
            {
                var blockRanges = new List<BlockRangeBase>();

                var sourceBlobName = currBlobItem;

                opProgress.StatusMessage = $"Working on {sourceBlobName}";

                logger.LogDebug($"START: {sourceBlobName}");

                if (sourceBlockList[currBlobItem].Count == 0)
                {
                    logger.LogError($"There are no block ranges for source item {sourceBlobName}; exiting!");
                    return false;
                }

                foreach (var newBlockRange in sourceBlockList[currBlobItem])
                {
                    // check if this block has already been copied + committed at the destination, and in that case, skip it
                    if (destBlockList.Contains(newBlockRange.Name))
                    {
                        logger.LogDebug($"Destination already has blockID {newBlockRange.Name} for source item {sourceBlobName}; skipping");

                        continue;
                    }
                    else
                    {
                        logger.LogDebug($"Adding blockID {newBlockRange.Name} for source item {sourceBlobName} to work list");

                        blockRanges.Add(newBlockRange);
                    }
                }

                logger.LogDebug($"Total number of of ranges to process for source item {sourceBlobName} is {blockRanges.Count}");

                // add this list of block IDs to the final list
                finalBlockList.AddRange(blockRanges.Select(e => e.Name));

                if (blockRanges.Count() > 0)
                {
                    // call the helper function to actually execute the writes to blob
                    var processBRStatus = await ProcessBlockRanges(blockRanges,
                        destBlob,
                        calcMD5ForBlock,
                        logger,
                        progress,
                        opProgress,
                        timeoutSeconds,
                        maxDOP,
                        useInbuiltRetry,
                        retryCount);

                    if (!processBRStatus)
                    {
                        logger.LogError($"One or more errors encountered when calling ProcessBlockRanges for source item {sourceBlobName}; exiting!");
                        return false;
                    }

                    // each iteration (each source item) we will commit an ever-increasing super-set of block IDs
                    // we do this to support "resume" operations later on.
                    // we will only do this if we actually did any work here TODO review
                    await BlobHelpers.GetStorageRetryPolicy($"PutBlockListAsync for blob {currBlobItem}", retryCount, logger).ExecuteAsync(async () =>
                    {
                        await destBlob.PutBlockListAsync(finalBlockList);
                    });
                }
                else
                {
                    logger.LogDebug($"There was no work to be done for source item {currBlobItem} as all blocks already existed in destination.");
                }

                logger.LogDebug($"END: {currBlobItem}");

                // report progress to caller
                opProgress.StatusMessage = $"Finished with {currBlobItem}";
                progress.Report(opProgress);
            }

            return true;
        }

        private async static Task<bool> ProcessBlockRanges(IEnumerable<BlockRangeBase> blockRangesToBeCopied,
            CloudBlockBlob destBlob,
            bool calcMD5ForBlock,
            ILogger logger,
            IProgress<OpProgress> progress,
            OpProgress progressDetails,
            int timeoutSeconds,
            int maxDOP,
            bool useInbuiltRetry,
            int retryCount)
        {
            bool allOK = true;

            var actionBlock = new ActionBlock<BlockRangeBase>(
            async (br) =>
            {
                var sw = Stopwatch.StartNew();
                try
                {
                    logger.LogDebug($"Inside the action block, before calling ProcessBlockRange for {br.Name}");

                    var brStatus = await ProcessBlockRange(br,
                        destBlob,
                        timeoutSeconds,
                        useInbuiltRetry,
                        retryCount,
                        calcMD5ForBlock,
                        logger);

                    if (!brStatus)
                    {
                        allOK = false;
                    }

                    logger.LogDebug($"Inside the action block, after calling ProcessBlockRange for {br.Name} with retval {brStatus}");

                    progress.Report(progressDetails);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, $"Could not process range with block ID {br.Name}");

                    allOK = false;
                }

                sw.Stop();

                logger.LogDebug("{eventType} {duration} {context}", "ProcessBlockRanges", sw.Elapsed, $"ProcessBlockRange {br.Name} ended.");
            },
            new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = maxDOP
            });

            foreach (var br in blockRangesToBeCopied)
            {
                await actionBlock.SendAsync(br);

                logger.LogDebug($"Sent block {br.Name} to the dataflow action block");

                // blockRangesDone++;
            }

            logger.LogDebug($"About to signal dataflow action block");

            actionBlock.Complete();

            logger.LogDebug($"Signaled dataflow action block");

            await actionBlock.Completion;

            logger.LogDebug($"Dataflow action block completed.");

            return allOK;
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
            bool useInbuiltRetry,
            int retryCount,
            bool calcMD5ForBlock,
            ILogger logger)
        {
            logger.LogDebug($"Started ProcessBlockRange for {currRange.Name}");

            using (var brData = await currRange.GetBlockRangeData(calcMD5ForBlock,
                timeoutSeconds,
                useInbuiltRetry,
                retryCount,
                logger))
            {
                var brDataIsNull = (brData is null) ? "null" : "valid";

                logger.LogDebug($"GetBlockRangeData was called for block {currRange.Name} and returned brData {brDataIsNull}");

                if (brData is null)
                {
                    logger.LogDebug($"Returning false from GetBlockRangeData for block {currRange.Name} as brData was {brDataIsNull}");

                    return false;
                }

                logger.LogDebug($"Inside ProcessBlockRange, about to start the PutBlockAsync action for {currRange.Name}");

                // use retry policy which will automatically handle the throttling related StorageExceptions
                await BlobHelpers.GetStorageRetryPolicy($"PutBlockAsync for block {currRange.Name}", retryCount, logger).ExecuteAsync(async () =>
                {
                    logger.LogDebug($"Before PutBlockAsync for {currRange.Name}");

                    var blobReqOpts = new BlobRequestOptions()
                    {
                        MaximumExecutionTime = TimeSpan.FromSeconds(timeoutSeconds)
                    };

                    if (!useInbuiltRetry)
                    {
                        blobReqOpts.RetryPolicy = new WindowsAzure.Storage.RetryPolicies.NoRetry();
                    }

                    // reset the memory stream again to 0
                    brData.MemStream.Position = 0;

                    // and then call Azure Storage to put this as a block with the given block ID and MD5 hash
                    await destBlob.PutBlockAsync(currRange.Name, brData.MemStream, brData.Base64EncodedMD5Checksum,
                                    null, blobReqOpts, null);

                    logger.LogDebug($"Finished PutBlockAsync for {currRange.Name}");
                });

                logger.LogDebug($"Finished ProcessBlockRange for {currRange.Name}");
            }

            return true;
        }
    }
}

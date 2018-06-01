namespace Microsoft.Azure.Samples.BlobCat
{
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Security.Cryptography;
    using System.Threading;
    using System.Threading.Tasks;

    public class BlobCatEngine
    {
        // currently 100 MB
        private static readonly int CHUNK_SIZE = 100 * 1024 * 1024;

        private static void GlobalOptimizations()
        {
            // First two are Best practices optimizations for Blob, as per the Azure Storage GitHub these are highly recommended.
            // The Threadpool setting - I had just played with it. Leaving it in here given many of the operations are effectively still sync
            ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 8;
            ServicePointManager.Expect100Continue = false;
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
        /// <param name="sourceBlobPrefix"></param>
        /// <param name="sortBlobs"></param>
        /// <param name="sourceBlobNames"></param>
        /// <param name="destStorageAccountName"></param>
        /// <param name="destStorageAccountKey"></param>
        /// <param name="destStorageContainerName"></param>
        /// <param name="destBlobName"></param>
        public static bool BlobToBlob(string sourceStorageAccountName,
            string sourceStorageContainerName,
            string sourceStorageAccountKey,
            string sourceBlobPrefix,
            bool sortBlobs,
            List<string> sourceBlobNames,
            string destStorageAccountName,
            string destStorageAccountKey,
            string destStorageContainerName,
            string destBlobName)
        {
            // this will be used to compute a unique blockId for the source blocks
            var shaHasher = new SHA384Managed();

            GlobalOptimizations();

            // TODO remove hard-coding of core.windows.net
            string sourceAzureStorageConnStr = $"DefaultEndpointsProtocol=https;AccountName={sourceStorageAccountName};AccountKey={sourceStorageAccountKey};EndpointSuffix=core.windows.net";
            string destAzureStorageConnStr = $"DefaultEndpointsProtocol=https;AccountName={destStorageAccountName};AccountKey={destStorageAccountKey};EndpointSuffix=core.windows.net";

            var destStorageAccount = CloudStorageAccount.Parse(destAzureStorageConnStr);
            var destBlobClient = destStorageAccount.CreateCloudBlobClient();
            var destContainer = destBlobClient.GetContainerReference(destStorageContainerName);
            var destBlob = destContainer.GetBlockBlobReference(destBlobName);
            
            List<string> destBlockList = new List<string>();

            // check if the blob exists, in which case we need to also get the list of blocks associated with that blob
            // this will help to skip blocks which were already completed, and thereby help with resume
            // TODO Block IDs are not unique across files - this will trip up the logic
            if (destBlob.ExistsAsync().GetAwaiter().GetResult())
            {
                // only get committed blocks to be sure 
                destBlockList = (from b in (destBlob.DownloadBlockListAsync(BlockListingFilter.Committed, null, null, null).GetAwaiter().GetResult()) select b.Name).ToList();
            }

            // create a place holder for the final block list (to be eventually used for put block list) and pre-populate it with the known list of blocks
            // already associated with the destination blob
            var finalBlockList = new List<string>();
            finalBlockList.AddRange(destBlockList);

            var sourceStorageAccount = CloudStorageAccount.Parse(sourceAzureStorageConnStr);
            var sourceBlobClient = sourceStorageAccount.CreateCloudBlobClient();
            var sourceContainer = sourceBlobClient.GetContainerReference(sourceStorageContainerName);

            var blobListing = new List<IListBlobItem>();
            BlobContinuationToken continuationToken = null;

            // check if there is a specific list of blobs given by the user, in which case the immediate 'if' code below will be skipped
            if (sourceBlobNames is null || sourceBlobNames.Count == 0)
            {
                // we have a prefix specified, so get a of blobs with a specific prefix and then add them to a list
                do
                {
                    var response = sourceContainer.ListBlobsSegmentedAsync(sourceBlobPrefix, true, BlobListingDetails.None, null, continuationToken, null, null).GetAwaiter().GetResult();
                    continuationToken = response.ContinuationToken;
                    blobListing.AddRange(response.Results);
                }
                while (continuationToken != null);

                // now just get the blob names, that's all we need for further processing
                sourceBlobNames = (from b in blobListing.OfType<CloudBlockBlob>() select b.Name).ToList();

                // if the user specified to sort the input blobs (only valid for the prefix case) then we will happily do that!
                // The gotcha here is that this is a string sort. So if blobs have names like blob_9, blob_13, blob_6, blob_3, blob_1
                // the sort order will result in blob_1, blob_13, blob_3, blob_6, blob_9. 
                // To avoid issues like this the user must 0-prefix the numbers embedded in the filenames.
                if (sortBlobs)
                {
                    sourceBlobNames.Sort();
                }
            }

            // iterate through each source blob, one at a time.
            foreach (var sourceBlobName in sourceBlobNames)
            {
                Debug.WriteLine($"{DateTime.Now}: START {sourceBlobName}");

                var sourceBlob = sourceContainer.GetBlockBlobReference(sourceBlobName);

                // first we get the block list of the source blob. we use this to later parallelize the download / copy operation
                var sourceBlockList = sourceBlob.DownloadBlockListAsync(BlockListingFilter.Committed, null, null, null).GetAwaiter().GetResult();

                var blockRanges = new List<BlockRange>();
                long currentOffset = 0;

                // iterate through the list of blocks and compute their effective offsets in the final file.
                int chunkIndex = 0;
                foreach (var blockListItem in sourceBlockList)
                {
                    // compute a unique blockId based on blob account + container + blob name (includes path) + block length + block "number"
                    // TODO also include the endpoint when we generalize for all clouds
                    var hashBasis = System.Text.Encoding.UTF8.GetBytes(string.Concat(sourceStorageAccountName, sourceStorageContainerName, sourceBlobName, blockListItem.Length, chunkIndex));
                    var newBlockId = Convert.ToBase64String(shaHasher.ComputeHash(hashBasis));

                    var newBlockRange = new BlockRange()
                    {
                        Name = newBlockId,
                        StartOffset = currentOffset,
                        Length = blockListItem.Length
                    };

                    // increment this here itself as we may potentially skip to the next blob
                    chunkIndex++;
                    currentOffset += blockListItem.Length;

                    // check if this block has already been copied + committed at the destination, and in that case, skip it
                    if (destBlockList.Contains(newBlockId))
                    {
                        continue;
                    }
                    else
                    {
                        blockRanges.Add(newBlockRange);
                    }
                }

                Debug.WriteLine($"Number of ranges: {blockRanges.Count}");

                // reset back to 0 to actually execute the copies
                currentOffset = 0;

                // proceed to copy blocks in parallel. to do this, we download to a local memory stream and then push that back out to the destination blob
                Parallel.ForEach<BlockRange, MD5CryptoServiceProvider>(blockRanges, new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount * 8 },
                    () =>
                {
                    // this will be a "task-local" copy. Better than creating this within the task, as that will be slighly expensive.
                    return new MD5CryptoServiceProvider();
                },
                (currRange, loopState, hasher) =>
                {
                    // TODO do we really need this copy?
                    var sourceBlobLocalCopy = sourceBlob; // sourceContainer.GetBlockBlobReference(sourceBlobName);

                    // TODO verify cast
                    using (var memStream = new MemoryStream((int)currRange.Length))
                    {
                        sourceBlobLocalCopy.DownloadRangeToStreamAsync(memStream, currRange.StartOffset, currRange.Length).GetAwaiter().GetResult();

                        // compute the hash, for which we need to reset the memory stream
                        memStream.Position = 0;
                        var md5Checksum = hasher.ComputeHash(memStream);

                        // reset the memory stream again to 0 and then call Azure Storage to put this as a block with the given block ID and MD5 hash
                        memStream.Position = 0;

                        Debug.WriteLine($"Putting block {currRange.Name}");

                        destBlob.PutBlockAsync(currRange.Name, memStream, Convert.ToBase64String(md5Checksum)).GetAwaiter().GetResult();
                    }

                    return hasher;
                },
                (hasherFinally) => { }
                );

                // keep adding the blocks we just copied, to the final block list
                finalBlockList.AddRange(from r in blockRanges select r.Name);

                // TODO review this whether we put this for each source file or at the end of all source files
                // when we are all done, execute a "commit" by using Put Block List operation
                destBlob.PutBlockListAsync(finalBlockList).GetAwaiter().GetResult();

                Debug.WriteLine($"{DateTime.Now}: END {sourceBlobName}");

                // TODO optionally allow user to specify extra character(s) to append in between files. This is typically needed when the source files do not have a trailing \n character.
            }

            // release the SHA hasher resources
            shaHasher.Dispose();

            // TODO handle failures.
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
        /// <param name="destStorageContainerName"></param>
        /// <param name="destBlobName"></param>
        /// <returns>
        /// True if successful; False if errors found
        /// </returns>
        public static bool DiskToBlob(string sourceFolderName,
            string sourceBlobPrefix,
            bool sortFiles,
            List<string> sourceFileNames,
            string destStorageAccountName,
            string destStorageAccountKey,
            string destStorageContainerName,
            string destBlobName)
        {
            GlobalOptimizations();

            // this will be used to compute a unique blockId for the blocks
            var shaHasher = new SHA384Managed();

            // TODO remove hard-coding of core.windows.net
            string destAzureStorageConnStr = $"DefaultEndpointsProtocol=https;AccountName={destStorageAccountName};AccountKey={destStorageAccountKey};EndpointSuffix=core.windows.net";

            var destStorageAccount = CloudStorageAccount.Parse(destAzureStorageConnStr);
            var destBlobClient = destStorageAccount.CreateCloudBlobClient();
            var destContainer = destBlobClient.GetContainerReference(destStorageContainerName);
            var destBlob = destContainer.GetBlockBlobReference(destBlobName);

            List<string> destBlockList = new List<string>();

            // check if the blob exists, in which case we need to also get the list of blocks associated with that blob
            // this will help to skip blocks which were already completed, and thereby help with resume
            // TODO Block IDs are not unique across files - this will trip up the logic
            if (destBlob.ExistsAsync().GetAwaiter().GetResult())
            {
                // only get committed blocks to be sure 
                destBlockList = (from b in (destBlob.DownloadBlockListAsync(BlockListingFilter.Committed, null, null, null).GetAwaiter().GetResult()) select b.Name).ToList();
            }

            // create a place holder for the final block list (to be eventually used for put block list) and pre-populate it with the known list of blocks
            // already associated with the destination blob
            var finalBlockList = new List<string>();
            finalBlockList.AddRange(destBlockList);

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
                Debug.WriteLine($"{DateTime.Now}: START {sourceFileName}");

                // we will split up the file into 100MB chunks.
                var blockRanges = new List<BlockRange>();
                long currentOffset = 0;

                var fInfo = new FileInfo(sourceFileName);

                // iterate through the list of blocks and compute their effective offsets in the final file.
                // TODO test with 0-sized and non-existing filenames
                int chunkIndex = 0;
                while(currentOffset < fInfo.Length)
                {
                    var currentChunkSize = Math.Min(CHUNK_SIZE, (fInfo.Length - currentOffset));

                    // compute a unique blockId based on full file path + file length + chunkNumber
                    var hashBasis = System.Text.Encoding.UTF8.GetBytes(string.Concat(sourceFileName, fInfo.Length, chunkIndex));
                    var newBlockId = Convert.ToBase64String(shaHasher.ComputeHash(hashBasis));

                    var newBlockRange = new BlockRange()
                    {
                        Name = newBlockId,
                        StartOffset = currentOffset,
                        Length = currentChunkSize
                    };

                    currentOffset += currentChunkSize;
                    chunkIndex++;

                    // only add a new block range if the blockID has not been commited in destination blob
                    // check if this block has already been copied + committed at the destination, and in that case, skip it
                    if (destBlockList.Contains(newBlockId))
                    {
                        continue;
                    }
                    else
                    {
                        blockRanges.Add(newBlockRange);
                    }
                }

                Debug.WriteLine($"Number of ranges: {blockRanges.Count}");

                // reset back to 0 to actually execute the copies
                currentOffset = 0;

                // proceed to copy blocks in parallel. to do this, we download to a local memory stream and then push that back out to the destination blob
                Parallel.ForEach<BlockRange, MD5CryptoServiceProvider>(blockRanges, new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount * 8 },
                    () =>
                    {
                        // this will be a "task-local" copy. Better than creating this within the task, as that will be slighly expensive.
                        return new MD5CryptoServiceProvider();
                    },
                (currRange, loopState, hasher) =>
                {
                    var backingArray = new byte[currRange.Length];
                    using (var memStream = new MemoryStream(backingArray))
                    {
                        // sourceBlobLocalCopy.DownloadRangeToStreamAsync(memStream, currRange.StartOffset, currRange.Length).GetAwaiter().GetResult();
                        using (var srcFile = new FileStream(sourceFileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                        {
                            Debug.WriteLine($"{DateTime.Now}: Started file segment @ {currRange.StartOffset}");

                            srcFile.Position = currRange.StartOffset;
                            srcFile.Read(backingArray, 0, (int)currRange.Length);

                            // compute the MD5 hash, for which we need to reset the memory stream
                            memStream.Position = 0;
                            var md5Checksum = hasher.ComputeHash(memStream);

                            // reset the memory stream again to 0 and then call Azure Storage to put this as a block with the given block ID and MD5 hash
                            memStream.Position = 0;

                            Debug.WriteLine($"Putting block {currRange.Name}");

                            destBlob.PutBlockAsync(currRange.Name, memStream, Convert.ToBase64String(md5Checksum)).GetAwaiter().GetResult();
                        }
                    }

                    return hasher;
                },
                (hasherFinally) => 
                {
                    hasherFinally.Dispose();
                }
                );

                // keep adding the blocks we just copied, to the final block list
                finalBlockList.AddRange(from r in blockRanges select r.Name);

                // TODO review this whether we put this for each source file or at the end of all source files
                // when we are all done, execute a "commit" by using Put Block List operation
                destBlob.PutBlockListAsync(finalBlockList).GetAwaiter().GetResult();

                Debug.WriteLine($"{DateTime.Now}: END {sourceFileName}");

                // TODO optionally allow user to specify extra character(s) to append in between files. This is typically needed when the source files do not have a trailing \n character.
            }

            // release the SHA hasher resources
            shaHasher.Dispose();

            // TODO handle failures.
            return true;
        }
    }
}

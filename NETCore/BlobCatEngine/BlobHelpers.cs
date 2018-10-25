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
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Polly;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net.Http;
    using System.Threading.Tasks;

    class BlobHelpers
    {
        /// <summary>
        /// Helper method to centralize the criteria for whether a storage exception is "retryable". 
        /// Currently we will retry whenever there is evidence of being throttled. Those are the string literals
        /// which are embedded herein.
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        private static bool IsStorageExceptionRetryable(StorageException ex)
        {
            var errorCode = ex.RequestInformation.ErrorCode;
            return (
            "ServerBusy" == errorCode
            || "InternalError" == errorCode
            || "OperationTimedOut" == errorCode
            || (ex.InnerException is HttpRequestException)
            || (ex.InnerException is OperationCanceledException)
            || (ex.InnerException is TimeoutException)
            || (ex.InnerException is IOException)
            );
        }

        /// <summary>
        /// Wrapper to centralize the storage retry policy definition; this is used in multiple places in the code
        /// </summary>
        /// <returns></returns>
        internal static Polly.Retry.RetryPolicy GetStorageRetryPolicy(string execContext, int retryCount, ILogger logger)
        {
            return Policy.Handle<StorageException>(ex => IsStorageExceptionRetryable(ex))
                    // TODO make retry count and sleep configurable???
                    .WaitAndRetryAsync(
                        retryCount,
                        retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                        (Exception genEx, TimeSpan timeSpan, Context context) =>
                        {
                            // TODO how can the below cast to StorageException be avoided; seems like Polly only allows generic Exception?
                            var ex = genEx as StorageException;

                            LogStorageException(execContext, ex, logger, true);
                        });
        }

        /// <summary>
        /// Helper to unwrap the StorageException and log additional details
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="logger"></param>
        internal static void LogStorageException(string context, StorageException ex, ILogger logger, bool retryable)
        {
            var exMsg = ex.RequestInformation.ExtendedErrorInformation == null ?
                ex.RequestInformation.Exception.ToString() : ex.RequestInformation.ExtendedErrorInformation.ErrorMessage;

            var errCode = ex.RequestInformation.ErrorCode;

            logger.Log(retryable ? Extensions.Logging.LogLevel.Warning : Extensions.Logging.LogLevel.Error,
                $"StorageException occured in context {context}. Exception message is {ex.Message} with Error code {errCode} and Extended Error Message {exMsg}.");

            if (!(ex.RequestInformation.ExtendedErrorInformation is null))
            {
                foreach (var addDetails in ex.RequestInformation.ExtendedErrorInformation.AdditionalDetails)
                {
                    logger.Log(retryable ? Extensions.Logging.LogLevel.Warning : Extensions.Logging.LogLevel.Error,
                        $"{addDetails.Key}: {addDetails.Value}");
                }
            }

            logger.LogDebug(ex, $"Storage Exception details for context {context}", null);

            return;
        }

        internal async static Task<IEnumerable<ListBlockItem>> GetBlockListForBlob(CloudBlockBlob destBlob, int retryCount, ILogger logger)
        {
            IEnumerable<ListBlockItem> retVal = null;

            // use retry policy which will automatically handle the throttling related StorageExceptions
            await GetStorageRetryPolicy($"DownloadBlockListAsync for blob {destBlob.Name}", retryCount, logger).ExecuteAsync(async () =>
            {
                if (!await destBlob.ExistsAsync())
                {
                    // obvious but setting explicitly
                    retVal = null;
                }
                else
                {
                    retVal = await destBlob.DownloadBlockListAsync(BlockListingFilter.Committed, null, null, null);
                }
            });

            return retVal;
        }

        /// <summary>
        /// Helper function to get a list of blobs with a specified prefix in their name
        /// </summary>
        /// <param name="inStorageAccountName"></param>
        /// <param name="inStorageContainerName"></param>
        /// <param name="inBlobPrefix"></param>
        /// <param name="inSAS"></param>
        /// <param name="inStorageAccountKey"></param>
        /// <param name="inEndpointSuffix"></param>
        /// <returns></returns>
        internal async static Task<List<string>> GetBlobListing(string inStorageAccountName,
            string inStorageContainerName,
            string inBlobPrefix,
            string inSAS,
            string inStorageAccountKey,
            string inEndpointSuffix,
            int retryCount,
            ILogger logger)
        {
            List<string> retVal = null;

            // use retry policy which will automatically handle the throttling related StorageExceptions
            await GetStorageRetryPolicy($"ListBlobsSegmentedAsync for blob prefix {inBlobPrefix}", retryCount, logger).ExecuteAsync(async () =>
            {
                var blobContainer = GetBlobContainerReference(inStorageAccountName,
                inStorageContainerName,
                inSAS,
                inStorageAccountKey,
                inEndpointSuffix,
                retryCount,
                logger);

                if (blobContainer != null)
                {
                    var blobListing = new List<IListBlobItem>();
                    BlobContinuationToken continuationToken = null;

                    // we have a prefix specified, so get a of blobs with a specific prefix and then add them to a list
                    do
                    {
                        var response = await blobContainer.ListBlobsSegmentedAsync(inBlobPrefix, true, BlobListingDetails.None, null, continuationToken, null, null);
                        continuationToken = response.ContinuationToken;
                        blobListing.AddRange(response.Results);
                    }
                    while (continuationToken != null);

                    retVal = (from b in blobListing.OfType<CloudBlockBlob>() select b.Name).ToList();
                }
            });

            return retVal;
        }

        /// <summary>
        /// Helper function to return a cloud block blob object
        /// </summary>
        /// <param name="inStorageAccountName"></param>
        /// <param name="inStorageContainerName"></param>
        /// <param name="inBlobName"></param>
        /// <param name="inSAS"></param>
        /// <param name="inStorageAccountKey"></param>
        /// <param name="inEndpointSuffix"></param>
        /// <returns></returns>

        internal static CloudBlockBlob GetBlockBlob(string inStorageAccountName,
            string inStorageContainerName,
            string inBlobName,
            string inSAS,
            string inStorageAccountKey,
            string inEndpointSuffix,
            int retryCount,
            ILogger logger)
        {
            CloudBlockBlob retVal = null;

            // use retry policy which will automatically handle the throttling related StorageExceptions
            GetStorageRetryPolicy($"GetBlockBlobReference for {inBlobName}", retryCount, logger).ExecuteAsync(async () =>
            {
                var blobContainer = GetBlobContainerReference(inStorageAccountName,
                inStorageContainerName,
                inSAS,
                inStorageAccountKey,
                inEndpointSuffix,
                retryCount,
                logger);

                retVal = blobContainer.GetBlockBlobReference(inBlobName);
            });

            return retVal;
        }

        /// <summary>
        /// Helper to return a CloudBlobContainer instance
        /// </summary>
        /// <param name="inStorageAccountName"></param>
        /// <param name="inStorageContainerName"></param>
        /// <param name="inSAS"></param>
        /// <param name="inStorageAccountKey"></param>
        /// <returns></returns>
        internal static CloudBlobContainer GetBlobContainerReference(string inStorageAccountName,
            string inStorageContainerName,
            string inSAS,
            string inStorageAccountKey,
            string inEndpointSuffix,
            int retryCount,
            ILogger logger)
        {
            CloudBlobContainer retVal = null;

            // use retry policy which will automatically handle the throttling related StorageExceptions
            GetStorageRetryPolicy($"GetContainerReference for container {inStorageContainerName}", retryCount, logger).ExecuteAsync(async () =>
            {
                var typeOfinCredential = string.IsNullOrEmpty(inSAS) ? "AccountKey" : "SharedAccessSignature";
                var inCredential = string.IsNullOrEmpty(inSAS) ? inStorageAccountKey : inSAS;

                string inAzureStorageConnStr = $"DefaultEndpointsProtocol=https;AccountName={inStorageAccountName};{typeOfinCredential}={inCredential};EndpointSuffix={inEndpointSuffix}";

                var inStorageAccount = CloudStorageAccount.Parse(inAzureStorageConnStr);
                var inBlobClient = inStorageAccount.CreateCloudBlobClient();

                retVal = inBlobClient.GetContainerReference(inStorageContainerName);
            });

            return retVal;
        }

        internal static async Task<(List<string> blockList, CloudBlockBlob destBlob)> GetDestinationBlobBlockList(string destStorageAccountName,
            string destStorageContainerName,
            string destBlobName,
            string destSAS,
            string destStorageAccountKey,
            string destEndpointSuffix,
            bool overwriteDest,
            int retryCount,
            ILogger logger)
        {
            var destBlockList = new List<string>();

            // get a reference to the destination blob
            var destBlob = BlobHelpers.GetBlockBlob(destStorageAccountName,
                destStorageContainerName,
                destBlobName,
                destSAS,
                destStorageAccountKey,
                destEndpointSuffix,
                retryCount,
                logger);

            if (destBlob is null)
            {
                logger.LogError($"Failed to get a reference to destination conatiner / blob {destBlobName}; exiting!");
                return (destBlockList, destBlob);
            }

            // check if the blob exists, in which case we need to also get the list of blocks associated with that blob
            // this will help to skip blocks which were already completed, and thereby help with resume
            var blockList = await BlobHelpers.GetBlockListForBlob(destBlob, retryCount, logger);
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

            return (destBlockList, destBlob);
        }
    }
}


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
        internal static Polly.Retry.RetryPolicy GetStorageRetryPolicy(string execContext, ILogger logger)
        {
            return Policy.Handle<StorageException>(ex => IsStorageExceptionRetryable(ex))
                    // TODO make retry count and sleep configurable???
                    .WaitAndRetryAsync(
                        5,
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

        internal async static Task<IEnumerable<ListBlockItem>> GetBlockListForBlob(CloudBlockBlob destBlob, ILogger logger)
        {
            IEnumerable<ListBlockItem> retVal = null;

            // use retry policy which will automatically handle the throttling related StorageExceptions
            await GetStorageRetryPolicy($"DownloadBlockListAsync for blob {destBlob.Name}", logger).ExecuteAsync(async () =>
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
            ILogger logger)
        {
            List<string> retVal = null;

            // use retry policy which will automatically handle the throttling related StorageExceptions
            await GetStorageRetryPolicy($"ListBlobsSegmentedAsync for blob prefix {inBlobPrefix}", logger).ExecuteAsync(async () =>
            {
                var blobContainer = GetBlobContainerReference(inStorageAccountName,
                inStorageContainerName,
                inSAS,
                inStorageAccountKey,
                inEndpointSuffix,
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
            ILogger logger)
        {
            CloudBlockBlob retVal = null;

            // use retry policy which will automatically handle the throttling related StorageExceptions
            GetStorageRetryPolicy($"GetBlockBlobReference for {inBlobName}", logger).ExecuteAsync(async () =>
            {
                var blobContainer = GetBlobContainerReference(inStorageAccountName,
                inStorageContainerName,
                inSAS,
                inStorageAccountKey,
                inEndpointSuffix,
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
            ILogger logger)
        {
            CloudBlobContainer retVal = null;

            // use retry policy which will automatically handle the throttling related StorageExceptions
            GetStorageRetryPolicy($"GetContainerReference for container {inStorageContainerName}", logger).ExecuteAsync(async () =>
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
    }
}


namespace Microsoft.Azure.Samples.BlobCat
{
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    class BlobHelpers
    {
        internal async static Task<IEnumerable<ListBlockItem>> GetBlockListForBlob(CloudBlockBlob destBlob)
        {
            if (!await destBlob.ExistsAsync())
            {
                return null;
            }

            // TODO retry policy
            return await destBlob.DownloadBlockListAsync(BlockListingFilter.Committed, null, null, null);
        }

        /// <summary>
        /// Helper function to get a list of blobs with a specified prefix in their name
        /// </summary>
        /// <param name="inStorageAccountName"></param>
        /// <param name="inStorageContainerName"></param>
        /// <param name="inBlobPrefix"></param>
        /// <param name="inSAS"></param>
        /// <param name="inStorageAccountKey"></param>
        /// <returns></returns>
        internal async static Task<List<string>> GetBlobListing(string inStorageAccountName,
            string inStorageContainerName,
            string inBlobPrefix,
            string inSAS,
            string inStorageAccountKey)
        {
            var blobContainer = GetBlobContainerReference(inStorageAccountName,
                inStorageContainerName,
                inSAS,
                inStorageAccountKey);

            var blobListing = new List<IListBlobItem>();
            BlobContinuationToken continuationToken = null;

            // we have a prefix specified, so get a of blobs with a specific prefix and then add them to a list
            do
            {
                var response = blobContainer.ListBlobsSegmentedAsync(inBlobPrefix, true, BlobListingDetails.None, null, continuationToken, null, null).GetAwaiter().GetResult();
                continuationToken = response.ContinuationToken;
                blobListing.AddRange(response.Results);
            }
            while (continuationToken != null);

            return (from b in blobListing.OfType<CloudBlockBlob>() select b.Name).ToList();
        }

        /// <summary>
        /// Helper function to return a cloud block blob object
        /// </summary>
        /// <param name="inStorageAccountName"></param>
        /// <param name="inStorageContainerName"></param>
        /// <param name="inBlobName"></param>
        /// <param name="inSAS"></param>
        /// <param name="inStorageAccountKey"></param>
        /// <returns></returns>

        internal static CloudBlockBlob GetBlockBlob(string inStorageAccountName,
            string inStorageContainerName,
            string inBlobName,
            string inSAS,
            string inStorageAccountKey)
        {
            var blobContainer = GetBlobContainerReference(inStorageAccountName,
                inStorageContainerName,
                inSAS,
                inStorageAccountKey);

            return blobContainer.GetBlockBlobReference(inBlobName);
        }

        /// <summary>
        /// Helper to return a CloudBlobContainer instance
        /// </summary>
        /// <param name="inStorageAccountName"></param>
        /// <param name="inStorageContainerName"></param>
        /// <param name="inSAS"></param>
        /// <param name="inStorageAccountKey"></param>
        /// <returns></returns>
        internal static CloudBlobContainer GetBlobContainerReference(string inStorageAccountName, string inStorageContainerName, string inSAS, string inStorageAccountKey)
        {
            var typeOfinCredential = string.IsNullOrEmpty(inSAS) ? "AccountKey" : "SharedAccessSignature";
            var inCredential = string.IsNullOrEmpty(inSAS) ? inStorageAccountKey : inSAS;

            // TODO remove hard-coding of core.windows.net
            string inAzureStorageConnStr = $"DefaultEndpointsProtocol=https;AccountName={inStorageAccountName};{typeOfinCredential}={inCredential};EndpointSuffix=core.windows.net";

            var inStorageAccount = CloudStorageAccount.Parse(inAzureStorageConnStr);
            var inBlobClient = inStorageAccount.CreateCloudBlobClient();

            return inBlobClient.GetContainerReference(inStorageContainerName);
        }
    }
}

using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Azure.Samples.BlobCat
{
    class BlobItem
    {
        internal bool useAsString = false;
        internal string sourceBlobName;
        internal CloudBlockBlob blockBlob;
        internal IEnumerable<ListBlockItem> blockList = new List<ListBlockItem>();
    }
}

using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Microsoft.Azure.Samples.BlobCat
{
    class BlockRangeData : IDisposable
    {
        internal MemoryStream MemStream;
        internal string Base64EncodedMD5Checksum;

        /// <summary>
        /// Dispose method
        /// </summary>
        public void Dispose()
        {
            MemStream.Dispose();
            GC.SuppressFinalize(this);
        }
    }

    class BlockRangeBase
    {
        internal long StartOffset;
        internal long Length;
        internal string Name;
                
        internal async virtual Task<BlockRangeData> GetBlockRangeData()
        {
            return null;
        }

        protected string ComputeChecksumFromStream(MemoryStream memStream)
        {
            // compute the MD5 hash, for which we need to reset the memory stream
            memStream.Position = 0;

            byte[] md5Checksum;

            using (var hasher = new MD5CryptoServiceProvider())
            {
                md5Checksum = hasher.ComputeHash(memStream);
            }

            // reset the position back to 0 before returning back to the caller
            memStream.Position = 0;

            return Convert.ToBase64String(md5Checksum);
        }

        protected void ComputeBlockId(string basis)
        {
            using (var shaHasher = new SHA384Managed())
            {
                this.Name = Convert.ToBase64String(shaHasher.ComputeHash(Encoding.UTF8.GetBytes(basis)));
            }
        }
    }

    class FileBlockRange : BlockRangeBase
    {
        private string sourceFileName;

        internal FileBlockRange(string inFilename, string hashBasis, long startOffset, long length)
        {
            sourceFileName = inFilename;
            ComputeBlockId(hashBasis);
            Length = length;
            StartOffset = startOffset;
        }

        internal async override Task<BlockRangeData> GetBlockRangeData()
        {
            var backingArray = new byte[this.Length];

            var memStream = new MemoryStream(backingArray);

            using (var srcFile = new FileStream(this.sourceFileName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                srcFile.Position = this.StartOffset;

                await srcFile.ReadAsync(backingArray, 0, (int)this.Length);

                var encodedChecksum = this.ComputeChecksumFromStream(memStream);

                return new BlockRangeData()
                {
                    MemStream = memStream,
                    Base64EncodedMD5Checksum = encodedChecksum
                };
            }
        }
    }

    class BlobBlockRange : BlockRangeBase
    {
        private CloudBlockBlob sourceBlob;

        internal BlobBlockRange(CloudBlockBlob inBlob, string hashBasis, long currOffset, long blockLength)
        {
            sourceBlob = inBlob;
            ComputeBlockId(hashBasis);
            StartOffset = currOffset;
            Length = blockLength;
        }

        internal async override Task<BlockRangeData> GetBlockRangeData()
        {
            // we do not wrap this around in a 'using' block because the caller will be calling Dispose() on the memory stream
            var memStream = new MemoryStream((int)this.Length);

            // TODO retry policy
            await this.sourceBlob.DownloadRangeToStreamAsync(memStream, this.StartOffset, this.Length);

            var encodedChecksum = this.ComputeChecksumFromStream(memStream);

            return new BlockRangeData()
            {
                MemStream = memStream,
                Base64EncodedMD5Checksum = encodedChecksum
            };
        }
    }

    class StringBlockRange : BlockRangeBase
    {
        private string literalValue;

        internal StringBlockRange(string inString, string hashBasis)
        {
            literalValue = inString;
            ComputeBlockId(hashBasis);
        }

        internal async override Task<BlockRangeData> GetBlockRangeData()
        {
            // we do not wrap this around in a 'using' block because the caller will be calling Dispose() on the memory stream
            var memStream = new MemoryStream(Encoding.UTF8.GetBytes(this.literalValue));

            var encodedChecksum = this.ComputeChecksumFromStream(memStream);

            return new BlockRangeData()
            {
                MemStream = memStream,
                Base64EncodedMD5Checksum = encodedChecksum
            };
        }
    }
}

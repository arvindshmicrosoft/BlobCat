﻿using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Data.HashFunction.CityHash;
using Microsoft.Extensions.Logging;

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
                
        internal async virtual Task<BlockRangeData> GetBlockRangeData(bool calcMD5ForBlock, int timeoutSeconds, ILogger logger)
        {
            // TODO in all derived classes we need to check for 0-length block ranges
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

            return Convert.ToBase64String(md5Checksum);
        }

        protected void ComputeBlockId(string basis)
        {
            var config = new CityHashConfig()
            {
                HashSizeInBits = 128
            };

            var hasher = CityHashFactory.Instance.Create(config);
            this.Name = hasher.ComputeHash(Encoding.UTF8.GetBytes(basis)).AsBase64String();

            //var config = new FNVConfig()
            //{
            //    HashSizeInBits = 384,
            //    Prime = 1239081328,
            //    Offset = 123123213
            //};

            //var hasher = FNV1aFactory.Instance.Create(config);
            //this.Name = hasher.ComputeHash(Encoding.UTF8.GetBytes(basis)).AsBase64String();

            // this.Name = Convert.ToBase64String(hasher.ComputeHash(Encoding.UTF8.GetBytes(basis)));
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

        internal async override Task<BlockRangeData> GetBlockRangeData(bool calcMD5ForBlock, int timeoutSeconds, ILogger logger)
        {
            // TODO logging
            var backingArray = new byte[this.Length];

            var memStream = new MemoryStream(backingArray);

            using (var srcFile = new FileStream(this.sourceFileName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                srcFile.Position = this.StartOffset;

                await srcFile.ReadAsync(backingArray, 0, (int)this.Length);

                var encodedChecksum = calcMD5ForBlock ? this.ComputeChecksumFromStream(memStream) : null;

                // reset the stream position back to 0
                memStream.Position = 0;

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

        internal async override Task<BlockRangeData> GetBlockRangeData(bool calcMD5ForBlock, int timeoutSeconds, ILogger logger)
        {
            BlockRangeData retVal = null;

            // use retry policy which will automatically handle the throttling related StorageExceptions
            await BlobHelpers.GetStorageRetryPolicy($"GetBlockRangeData for source blob {this.sourceBlob.Name} corresponding to block Id {this.Name}", logger).ExecuteAsync(async () =>
            {
                bool exOccured = false;

                // we do not wrap this around in a 'using' block because the caller will be calling Dispose() on the memory stream
                var memStream = new MemoryStream((int)this.Length);

                logger.LogDebug($"Inside GetBlockRangeData; about to call DownloadRangeToStreamAsync for {this.Name}");

                try
                {
                    await this.sourceBlob.DownloadRangeToStreamAsync(memStream,
                        this.StartOffset,
                        this.Length,
                        null,
                        new BlobRequestOptions()
                        {
                            // TODO should retry
                            RetryPolicy = new WindowsAzure.Storage.RetryPolicies.NoRetry(),
                            ServerTimeout = TimeSpan.FromSeconds(timeoutSeconds),
                            MaximumExecutionTime = TimeSpan.FromSeconds(timeoutSeconds)
                        },
                        null);
                }
                catch(Exception ex)
                {
                    logger.LogCritical(ex, $"Unhandled exception in DownloadRangeToStreamAsync for source blob {this.sourceBlob.Name} corresponding to block Id {this.Name}", null);
                    exOccured = true;
                }

                logger.LogDebug($"Inside GetBlockRangeData; finished DownloadRangeToStreamAsync for {this.Name}");

                if (!exOccured)
                {
                    var encodedChecksum = calcMD5ForBlock ? this.ComputeChecksumFromStream(memStream) : null;

                    // reset the stream position back to 0
                    memStream.Position = 0;

                    retVal = new BlockRangeData()
                    {
                        MemStream = memStream,
                        Base64EncodedMD5Checksum = encodedChecksum
                    };
                }
            });

            return retVal;
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

        internal async override Task<BlockRangeData> GetBlockRangeData(bool calcMD5ForBlock, int timeoutSeconds, ILogger logger)
        {
            // TODO logging

            // we do not wrap this around in a 'using' block because the caller will be calling Dispose() on the memory stream
            var memStream = new MemoryStream(Encoding.UTF8.GetBytes(this.literalValue));

            var encodedChecksum = calcMD5ForBlock ? this.ComputeChecksumFromStream(memStream) : null;

            // reset the stream position back to 0
            memStream.Position = 0;

            return new BlockRangeData()
            {
                MemStream = memStream,
                Base64EncodedMD5Checksum = encodedChecksum
            };
        }
    }
}

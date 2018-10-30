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
    using System.Data.HashFunction.CityHash;
    using System.IO;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading.Tasks;

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

    /// <summary>
    /// Base class from which the different types of "ranges" are derived: blob, file and string
    /// </summary>
    class BlockRangeBase
    {
        internal long StartOffset;
        internal long Length;
        internal string Name;
        internal string Parent;

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        internal async virtual Task<BlockRangeData> GetBlockRangeData(bool calcMD5ForBlock, int timeoutSeconds, bool useInbuiltRetry, int retryCount, ILogger logger)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
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
        }
    }

    /// <summary>
    /// Represents a file "range" stream
    /// </summary>
    class FileBlockRange : BlockRangeBase
    {
        internal FileBlockRange(string inFilename, string hashBasis, long startOffset, long length)
        {
            Parent = inFilename;
            ComputeBlockId(hashBasis);
            Length = length;
            StartOffset = startOffset;
        }

        /// <summary>
        /// Gets a stream object which represents the "block" for the current file
        /// </summary>
        /// <param name="calcMD5ForBlock"></param>
        /// <param name="timeoutSeconds"></param>
        /// <param name="useInbuiltRetry"></param>
        /// <param name="retryCount"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        internal async override Task<BlockRangeData> GetBlockRangeData(bool calcMD5ForBlock, int timeoutSeconds, bool useInbuiltRetry, int retryCount, ILogger logger)
        {
            var backingArray = new byte[this.Length];

            var memStream = new MemoryStream(backingArray);

            using (var srcFile = new FileStream(this.Parent, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                srcFile.Position = this.StartOffset;

                logger.LogDebug($"Inside FileBlockRange::GetBlockRangeData; about to call ReadAsync for {this.Name}");

                await srcFile.ReadAsync(backingArray, 0, (int)this.Length);

                logger.LogDebug($"FileBlockRange::GetBlockRangeData: finished call to ReadAsync for {this.Name}");

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

    /// <summary>
    /// Represents a Blob "range" stream
    /// </summary>
    class BlobBlockRange : BlockRangeBase
    {
        private CloudBlockBlob sourceBlob;

        internal BlobBlockRange(CloudBlockBlob inBlob, string hashBasis, long currOffset, long blockLength)
        {
            Parent = inBlob.Name;
            sourceBlob = inBlob;
            ComputeBlockId(hashBasis);
            StartOffset = currOffset;
            Length = blockLength;
        }

        internal async override Task<BlockRangeData> GetBlockRangeData(bool calcMD5ForBlock, int timeoutSeconds, bool useInbuiltRetry, int retryCount, ILogger logger)
        {
            BlockRangeData retVal = null;

            // use retry policy which will automatically handle the throttling related StorageExceptions
            await BlobHelpers.GetStorageRetryPolicy($"BlobBlockRange::GetBlockRangeData for source blob {this.sourceBlob.Name} corresponding to block Id {this.Name}", retryCount, logger).ExecuteAsync(async () =>
            {
                // we do not wrap this around in a 'using' block because the caller will be calling Dispose() on the memory stream
                var memStream = new MemoryStream((int)this.Length);

                logger.LogDebug($"Inside BlobBlockRange::GetBlockRangeData; about to call DownloadRangeToStreamAsync for {this.Name}");

                var blobReqOpts = new BlobRequestOptions()
                {
                    ServerTimeout = TimeSpan.FromSeconds(timeoutSeconds),
                    MaximumExecutionTime = TimeSpan.FromSeconds(timeoutSeconds)
                };

                if (!useInbuiltRetry)
                {
                    blobReqOpts.RetryPolicy = new WindowsAzure.Storage.RetryPolicies.NoRetry();
                }

                await this.sourceBlob.DownloadRangeToStreamAsync(memStream,
                    this.StartOffset,
                    this.Length,
                    null,
                    blobReqOpts,
                    null);

                logger.LogDebug($"BlobBlockRange::GetBlockRangeData finished DownloadRangeToStreamAsync for {this.Name}");

                var encodedChecksum = calcMD5ForBlock ? this.ComputeChecksumFromStream(memStream) : null;

                // reset the stream position back to 0
                memStream.Position = 0;

                retVal = new BlockRangeData()
                {
                    MemStream = memStream,
                    Base64EncodedMD5Checksum = encodedChecksum
                };
            });

            return retVal;
        }
    }

    /// <summary>
    /// Represents a string "range" stream
    /// </summary>
    class StringBlockRange : BlockRangeBase
    {
        private string literalValue;

        internal StringBlockRange(string inString, string hashBasis)
        {
            literalValue = inString;
            ComputeBlockId(hashBasis);
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        internal async override Task<BlockRangeData> GetBlockRangeData(bool calcMD5ForBlock, int timeoutSeconds, bool useInbuiltRetry, int retryCount, ILogger logger)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
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

using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Azure.Samples.BlobCat
{
    class BlockRange
    {
        internal long StartOffset;
        internal long Length;
        internal string Name;
        internal string StringToUse = null;
    }
}

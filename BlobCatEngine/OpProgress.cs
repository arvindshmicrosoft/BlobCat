using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Azure.Samples.BlobCat
{
    public class OpProgress
    {
        public double Percent;
        public string StatusMessage;
        public int TotalTicks = 0;
    }
}

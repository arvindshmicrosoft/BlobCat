namespace Microsoft.Azure.Samples.BlobCat
{
    using System;
    using System.Runtime.InteropServices;

    class Win32Helpers
    {
        [DllImport("kernel32.dll")]
        static extern bool SetFileValidData(IntPtr hFile, UInt64 ValidDataLength);

        [DllImport("kernel32.dll")]
        static extern bool SetFilePointerEx(IntPtr hFile, long liDistanceToMove, IntPtr lpNewFilePointer, uint dwMoveMethod);
    }
}

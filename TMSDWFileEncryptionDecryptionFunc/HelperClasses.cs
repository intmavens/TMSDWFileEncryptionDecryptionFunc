using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TMSDWFileEncryptionDecryptionFunc
{
    public class ColumnCryptoghapherRequest
    {
        public string InputBlob { get; set; }
        public string OutputBlob { get; set; }
        public Dictionary<string, string> columnsToEncryptOrDecrypt { get; set; }

    }

    public class FullFileEncryptionDecryptionRequest
    {
        public string InputBlob { get; set; }
        public string OutputBlob { get; set; }

    }

}

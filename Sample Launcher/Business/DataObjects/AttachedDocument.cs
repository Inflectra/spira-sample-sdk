using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Inflectra.Rapise.RapiseLauncher.Business.DataObjects
{
    /// <summary>
    /// Represents a file/url attachment associated with a test case
    /// </summary>
    public class AttachedDocument
    {
        /// <summary>
        /// The ids of the different attachment types
        /// </summary>
        public enum AttachmentType
        {
            File = 1,
            URL = 2
        }

        /// <summary>
        /// The type of attachment
        /// </summary>
        public AttachmentType Type { get; set; }

        /// <summary>
        /// The URL or filename of  the attachment
        /// </summary>
        public string FilenameOrUrl { get; set; }

        /// <summary>
        /// The id of the test case
        /// </summary>
        public int TestCaseId { get; set; }

        /// <summary>
        /// The id of the document type
        /// </summary>
        public int ProjectAttachmentTypeId { get; set; }

        /// <summary>
        /// The id of the document folder
        /// </summary>
        public int ProjectAttachmentFolderId { get; set; }

        /// <summary>
        /// The size of the attachment
        /// </summary>
        public int Size { get; set; }

        /// <summary>
        /// Any provided meta-tags (comma-separated)
        /// </summary>
        public string Tags { get; set; }

        /// <summary>
        /// The date it was uploaded
        /// </summary>
        public DateTime UploadDate { get; set; }

        /// <summary>
        /// The date it was last edited
        /// </summary>
        public DateTime EditedDate { get; set; }

        /// <summary>
        /// The current version
        /// </summary>
        public string CurrentVersion { get; set; }

        /// <summary>
        /// The physical contents of the attachment
        /// </summary>
        public byte[] BinaryData { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SampleDataSync
{
    /// <summary>
    /// Contains helper-functions used by the data-sync
    /// </summary>
    public static class InternalFunctions
    {
        /// <summary>
        /// Finds a mapping entry from the internal id and project id
        /// </summary>
        /// <param name="projectId">The project id</param>
        /// <param name="internalId">The internal id</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <returns>The matching entry or Null if none found</returns>
        public static SpiraImportExport.RemoteDataMapping FindMappingByInternalId(int projectId, int internalId, SpiraImportExport.RemoteDataMapping[] dataMappings)
        {
            foreach (SpiraImportExport.RemoteDataMapping dataMapping in dataMappings)
            {
                if (dataMapping.InternalId == internalId && dataMapping.ProjectId == projectId)
                {
                    return dataMapping;
                }
            }
            return null;
        }

        /// <summary>
        /// Finds a mapping entry from the external key and project id
        /// </summary>
        /// <param name="projectId">The project id</param>
        /// <param name="externalKey">The external key</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <param name="onlyPrimaryEntries">Do we only want to locate primary entries</param>
        /// <returns>The matching entry or Null if none found</returns>
        public static SpiraImportExport.RemoteDataMapping FindMappingByExternalKey(int projectId, string externalKey, SpiraImportExport.RemoteDataMapping[] dataMappings, bool onlyPrimaryEntries)
        {
            foreach (SpiraImportExport.RemoteDataMapping dataMapping in dataMappings)
            {
                if (dataMapping.ExternalKey == externalKey && dataMapping.ProjectId == projectId)
                {
                    //See if we're only meant to return primary entries
                    if (!onlyPrimaryEntries || dataMapping.Primary)
                    {
                        return dataMapping;
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Finds a mapping entry from the internal id
        /// </summary>
        /// <param name="internalId">The internal id</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <returns>The matching entry or Null if none found</returns>
        /// <remarks>Used when no project id stored in the mapping collection</remarks>
        public static SpiraImportExport.RemoteDataMapping FindMappingByInternalId(int internalId, SpiraImportExport.RemoteDataMapping[] dataMappings)
        {
            foreach (SpiraImportExport.RemoteDataMapping dataMapping in dataMappings)
            {
                if (dataMapping.InternalId == internalId)
                {
                    return dataMapping;
                }
            }
            return null;
        }

        /// <summary>
        /// Finds a mapping entry from the external key
        /// </summary>
        /// <param name="externalKey">The external key</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <returns>The matching entry or Null if none found</returns>
        /// <remarks>Used when no project id stored in the mapping collection</remarks>
        public static SpiraImportExport.RemoteDataMapping FindMappingByExternalKey(string externalKey, SpiraImportExport.RemoteDataMapping[] dataMappings)
        {
            foreach (SpiraImportExport.RemoteDataMapping dataMapping in dataMappings)
            {
                if (dataMapping.ExternalKey == externalKey)
                {
                    return dataMapping;
                }
            }
            return null;
        }

        /// <summary>
        /// Extracts the matching custom property text value from an artifact
        /// </summary>
        /// <param name="remoteArtifact">The artifact</param>
        /// <param name="customPropertyName">The name of the custom property</param>
        /// <returns></returns>
        public static String GetCustomPropertyTextValue(SpiraImportExport.RemoteArtifact remoteArtifact, string customPropertyName)
        {
            if (customPropertyName == "TEXT_01")
            {
                return remoteArtifact.Text01;
            }
            if (customPropertyName == "TEXT_02")
            {
                return remoteArtifact.Text02;
            }
            if (customPropertyName == "TEXT_03")
            {
                return remoteArtifact.Text03;
            }
            if (customPropertyName == "TEXT_04")
            {
                return remoteArtifact.Text04;
            }
            if (customPropertyName == "TEXT_05")
            {
                return remoteArtifact.Text05;
            }
            if (customPropertyName == "TEXT_06")
            {
                return remoteArtifact.Text06;
            }
            if (customPropertyName == "TEXT_07")
            {
                return remoteArtifact.Text07;
            }
            if (customPropertyName == "TEXT_08")
            {
                return remoteArtifact.Text08;
            }
            if (customPropertyName == "TEXT_09")
            {
                return remoteArtifact.Text09;
            }
            if (customPropertyName == "TEXT_10")
            {
                return remoteArtifact.Text10;
            }
            return null;
        }

        /// <summary>
        /// Extracts the matching custom property list value from an artifact
        /// </summary>
        /// <param name="remoteArtifact">The artifact</param>
        /// <param name="customPropertyName">The name of the custom property</param>
        /// <returns></returns>
        public static Nullable<int> GetCustomPropertyListValue(SpiraImportExport.RemoteArtifact remoteArtifact, string customPropertyName)
        {
            if (customPropertyName == "LIST_01")
            {
                return remoteArtifact.List01;
            }
            if (customPropertyName == "LIST_02")
            {
                return remoteArtifact.List02;
            }
            if (customPropertyName == "LIST_03")
            {
                return remoteArtifact.List03;
            }
            if (customPropertyName == "LIST_04")
            {
                return remoteArtifact.List04;
            }
            if (customPropertyName == "LIST_05")
            {
                return remoteArtifact.List05;
            }
            if (customPropertyName == "LIST_06")
            {
                return remoteArtifact.List06;
            }
            if (customPropertyName == "LIST_07")
            {
                return remoteArtifact.List07;
            }
            if (customPropertyName == "LIST_08")
            {
                return remoteArtifact.List08;
            }
            if (customPropertyName == "LIST_09")
            {
                return remoteArtifact.List09;
            }
            if (customPropertyName == "LIST_10")
            {
                return remoteArtifact.List10;
            }
            return null;
        }

        /// <summary>
        /// Sets the matching custom property text value on an artifact
        /// </summary>
        /// <param name="remoteArtifact">The artifact</param>
        /// <param name="customPropertyName">The name of the custom property</param>
        /// <param name="value">The value to set</param>
        /// <returns></returns>
        public static void SetCustomPropertyTextValue(SpiraImportExport.RemoteArtifact remoteArtifact, string customPropertyName, string value)
        {
            if (customPropertyName == "TEXT_01")
            {
                remoteArtifact.Text01 = value;
            }
            if (customPropertyName == "TEXT_02")
            {
                remoteArtifact.Text02 = value;
            }
            if (customPropertyName == "TEXT_03")
            {
                remoteArtifact.Text03 = value;
            }
            if (customPropertyName == "TEXT_04")
            {
                remoteArtifact.Text04 = value;
            }
            if (customPropertyName == "TEXT_05")
            {
                remoteArtifact.Text05 = value;
            }
            if (customPropertyName == "TEXT_06")
            {
                remoteArtifact.Text06 = value;
            }
            if (customPropertyName == "TEXT_07")
            {
                remoteArtifact.Text07 = value;
            }
            if (customPropertyName == "TEXT_08")
            {
                remoteArtifact.Text08 = value;
            }
            if (customPropertyName == "TEXT_09")
            {
                remoteArtifact.Text09 = value;
            }
            if (customPropertyName == "TEXT_10")
            {
                remoteArtifact.Text10 = value;
            }
        }

        /// <summary>
        /// Sets the matching custom property list value on an artifact
        /// </summary>
        /// <param name="remoteArtifact">The artifact</param>
        /// <param name="customPropertyName">The name of the custom property</param>
        /// <param name="value">The value to set</param>
        /// <returns></returns>
        public static void SetCustomPropertyListValue(SpiraImportExport.RemoteArtifact remoteArtifact, string customPropertyName, Nullable<int> value)
        {
            if (customPropertyName == "LIST_01")
            {
                remoteArtifact.List01 = value;
            }
            if (customPropertyName == "LIST_02")
            {
                remoteArtifact.List02 = value;
            }
            if (customPropertyName == "LIST_03")
            {
                remoteArtifact.List03 = value;
            }
            if (customPropertyName == "LIST_04")
            {
                remoteArtifact.List04 = value;
            }
            if (customPropertyName == "LIST_05")
            {
                remoteArtifact.List05 = value;
            }
            if (customPropertyName == "LIST_06")
            {
                remoteArtifact.List06 = value;
            }
            if (customPropertyName == "LIST_07")
            {
                remoteArtifact.List07 = value;
            }
            if (customPropertyName == "LIST_08")
            {
                remoteArtifact.List08 = value;
            }
            if (customPropertyName == "LIST_09")
            {
                remoteArtifact.List09 = value;
            }
            if (customPropertyName == "LIST_10")
            {
                remoteArtifact.List10 = value;
            }
        }

        /// <summary>
        /// Renders HTML content as plain text, since JIRA cannot handle tags
        /// </summary>
        /// <param name="source">The HTML markup</param>
        /// <returns>Plain text representation</returns>
        /// <remarks>Handles line-breaks, etc.</remarks>
        public static string HtmlRenderAsPlainText(string source)
        {
            try
            {
                string result;

                // Remove HTML Development formatting
                // Replace line breaks with space
                // because browsers inserts space
                result = source.Replace("\r", " ");
                // Replace line breaks with space
                // because browsers inserts space
                result = result.Replace("\n", " ");
                // Remove step-formatting
                result = result.Replace("\t", string.Empty);
                // Remove repeating speces becuase browsers ignore them
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"( )+", " ");

                // Remove the header (prepare first by clearing attributes)
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*head([^>])*>", "<head>",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"(<( )*(/)( )*head( )*>)", "</head>",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(<head>).*(</head>)", string.Empty,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // remove all scripts (prepare first by clearing attributes)
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*script([^>])*>", "<script>",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"(<( )*(/)( )*script( )*>)", "</script>",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                //result = System.Text.RegularExpressions.Regex.Replace(result, 
                //         @"(<script>)([^(<script>\.</script>)])*(</script>)",
                //         string.Empty, 
                //         System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"(<script>).*(</script>)", string.Empty,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // remove all styles (prepare first by clearing attributes)
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*style([^>])*>", "<style>",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"(<( )*(/)( )*style( )*>)", "</style>",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(<style>).*(</style>)", string.Empty,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // insert tabs in spaces of <td> tags
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*td([^>])*>", "\t",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // insert line breaks in places of <BR> and <LI> tags
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*br( )*>", "\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*li( )*>", "\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // insert line paragraphs (double line breaks) in place
                // if <P>, <DIV> and <TR> tags
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*div([^>])*>", "\r\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*tr([^>])*>", "\r\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*p([^>])*>", "\r\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // Remove remaining tags like <a>, links, images,
                // comments etc - anything thats enclosed inside < >
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<[^>]*>", string.Empty,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // replace special characters:
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&nbsp;", " ",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&bull;", " * ",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&lsaquo;", "<",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&rsaquo;", ">",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&trade;", "(tm)",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&frasl;", "/",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<", "<",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @">", ">",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&copy;", "(c)",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&reg;", "(r)",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                // Remove all others. More can be added, see
                // http://hotwired.lycos.com/webmonkey/reference/special_characters/
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&(.{2,6});", string.Empty,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // for testng
                //System.Text.RegularExpressions.Regex.Replace(result, 
                //       this.txtRegex.Text,string.Empty, 
                //       System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // make line breaking consistent
                result = result.Replace("\n", "\r");

                // Remove extra line breaks and tabs:
                // replace over 2 breaks with 2 and over 4 tabs with 4. 
                // Prepare first to remove any whitespaces inbetween
                // the escaped characters and remove redundant tabs inbetween linebreaks
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(\r)( )+(\r)", "\r\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(\t)( )+(\t)", "\t\t",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(\t)( )+(\r)", "\t\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(\r)( )+(\t)", "\r\t",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                // Remove redundant tabs
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(\r)(\t)+(\r)", "\r\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                // Remove multible tabs followind a linebreak with just one tab
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(\r)(\t)+", "\r\t",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                // Initial replacement target string for linebreaks
                string breaks = "\r\r\r";
                // Initial replacement target string for tabs
                string tabs = "\t\t\t\t\t";
                for (int index = 0; index < result.Length; index++)
                {
                    result = result.Replace(breaks, "\r\r");
                    result = result.Replace(tabs, "\t\t\t\t");
                    breaks = breaks + "\r";
                    tabs = tabs + "\t";
                }

                // Thats it.
                return result;

            }
            catch
            {
                return source;
            }
        }
    }
}

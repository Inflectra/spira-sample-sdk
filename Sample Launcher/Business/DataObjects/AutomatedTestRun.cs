using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Inflectra.Rapise.RapiseLauncher.Business.DataObjects
{
	/// <summary>
	/// Represents an automated test run in the system
	/// </summary>
	public class AutomatedTestRun : TestRun
	{
		/// <summary>
		/// The name of the external automated tool that executed the test
		/// </summary>
		public string RunnerName;

		/// <summary>
		/// The name of the test case as it is known in the external tool
		/// </summary>
		public string RunnerTestName;

		/// <summary>
		/// The number of assertions/errors reported during the automated test execution
		/// </summary>
		public Nullable<int> RunnerAssertCount;

		/// <summary>
		/// The summary result of the test case
		/// </summary>
		public string RunnerMessage;

		/// <summary>
		/// The detailed trace of test results reported back from the automated testing tool
		/// </summary>
		public string RunnerStackTrace;

		/// <summary>
		/// The id of the automation host that the result is being recorded for
		/// </summary>
		public Nullable<int> AutomationHostId;

		/// <summary>
		/// The id of the automation engine that the result is being recorded for
		/// </summary>
		public Nullable<int> AutomationEngineId;

		/// <summary>
		/// The extension token of the automation engine that the result is being recorded for
		/// </summary>
		public string AutomationEngineToken;

		/// <summary>
		/// The filename of the test script (if an embedded attachment) or the full filepath/URL if it's a linked test script
		/// </summary>
		public string FilenameOrUrl;

		/// <summary>
		/// The size of the test script in bytes
		/// </summary>
		/// <remarks>
		/// Will be 0 for a linked test script
		/// </remarks>
		public int Size;

		/// <summary>
		/// The type of test script (1=File, 2=URL)
		/// </summary>
		public AttachmentType Type;

		/// <summary>
		/// The actual test script (null if a linked test script)
		/// </summary>
		public byte[] TestScript;

		/// <summary>
		/// The list of test run parameters that have been provided
		/// </summary>
		public List<TestRunParameter> Parameters;

		/// <summary>
		/// Enumeration of attachment types
		/// </summary>
		public enum AttachmentType : int
		{
			File = 1,
			URL = 2
		}
	}
}
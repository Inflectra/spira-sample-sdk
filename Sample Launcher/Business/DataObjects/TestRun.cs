using System;
using System.Data;
using System.Collections.Generic;
using System.Configuration;

namespace Inflectra.Rapise.RapiseLauncher.Business.DataObjects
{
	/// <summary>
	/// This object represents a single test run instance in the system
	/// </summary>
	public class TestRun : Artifact
	{
		/// <summary>
		/// The id of the test run
		/// </summary>
		public Guid TestRunId;

		/// <summary>
		/// The name of the test run (usually the same as the test case)
		/// </summary>
		public string Name;

		/// <summary>
		/// The id of the test case that the test run is an instance of
		/// </summary>
		public int TestCaseId;

		/// <summary>
		/// The id of the type of test run (automated vs. manual)
		/// </summary>
		public int TestRunTypeId;

		/// <summary>
		/// The id of the user that executed the test
		/// </summary>
		/// <remarks>
		/// The authenticated user is used if no value is provided
		/// </remarks>
		public Nullable<int> TesterId;

		/// <summary>
		/// The id of overall execution status for the test run
		/// </summary>
		/// <remarks>
		/// Failed = 1;
		/// Passed = 2;
		/// NotRun = 3;
		/// NotApplicable = 4;
		/// Blocked = 5;
		/// Caution = 6;
		/// </remarks>
		public TestStatusEnum ExecutionStatus;

		/// <summary>
		/// The id of the release that the test run should be reported against
		/// </summary>
		public Nullable<int> ReleaseId;

		/// <summary>
		/// The id of the test set that the test run should be reported against
		/// </summary>
		public Nullable<int> TestSetId;

		/// <summary>
		/// The id of the unique test case entry in the test set
		/// </summary>
		public Nullable<int> TestSetTestCaseId;

		/// <summary>
		/// The date/time that the test execution was started
		/// </summary>
		public DateTime StartDate;

		/// <summary>
		/// The date/time that the test execution was completed
		/// </summary>
		public Nullable<DateTime> EndDate;

		/// <summary>
		/// Enumeration of test statuses.
		/// </summary>
		public enum TestStatusEnum :int
		{
			Failed = 1,
			Passed = 2,
			NotRun = 3,
			NotApplicable = 4,
			Blocked = 5,
			Caution = 6
		}
	}
}

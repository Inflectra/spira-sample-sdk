using System;
using System.Collections.Generic;
using System.Linq;

using Inflectra.Rapise.RapiseLauncher.Business.SpiraImportExport;

namespace Inflectra.Rapise.RapiseLauncher.Business
{
	/// <summary>
	/// This is used in the frmConfig to generate collections of test sets from the supplied
	/// RemoteAutomatedTestRun objects gotten from the SpiraTest server.
	/// </summary>
	public class PendingSet : IComparable
	{
		#region Internal Vars
		private int _testSetId = -1;
		private int _projectId = -1;
		private string _name;
		private List<RemoteAutomatedTestRun> _testRuns;
		private PendingSetRunStatusEnum _status;
		#endregion

		/// <summary>Creates a new instance of the class.</summary>
		public PendingSet(string name)
		{
			this._testRuns = new List<RemoteAutomatedTestRun>();
			this.ScheduledDate = null;
			this._testSetId = -1;
			this._name = name;
			this._status = PendingSetRunStatusEnum.NotStarted;
		}

		/// <summary>Creates a new instance of the class, setting the TestSetId</summary>
		/// <param name="testSetId">The TestSetId of this PendingSet</param>
		public PendingSet(string name, int testSetId)
			: this(name)
		{
			this._testSetId = testSetId;
		}

		/// <summary>Creates a new instance of the class.</summary>
		/// <param name="testRun">The first RemoteAutomatedTestRun for the list.</param>
		public PendingSet(string name, RemoteAutomatedTestRun testRun)
			: this(name)
		{
			this.AddPendingTestRun(testRun);
		}

		/// <summary>Creates a new instance of the class.</summary>
		/// <param name="scheduledTime">The scheduled time for the PendingSet.</param>
		public PendingSet(string name, DateTime scheduledTime)
			: this(name)
		{
			this.ScheduledDate = scheduledTime;
		}

		#region Methods
		/// <summary>Add a single RemoteAutomatedTestRun to the collection.</summary>
		/// <param name="testRun">The testRun to add.</param>
		/// <returns>The current number of testruns in this set.</returns>
		public int AddPendingTestRun(RemoteAutomatedTestRun testRun)
		{
			if (this._testRuns == null)
				this._testRuns = new List<RemoteAutomatedTestRun>();

			if (this._testRuns.Count == 0)
			{
				this.ScheduledDate = testRun.ScheduledDate;
				if (this.TestSetId == -1) this._testSetId = testRun.TestSetId.Value;
				if (this.ProjectId == -1) this._projectId = testRun.ProjectId.Value;
			}

			this._testRuns.Add(testRun);

			return this.Count;
		}

		/// <summary>Whether or not the given object is the same as this one. Checks TestSetId only.</summary>
		/// <param name="obj">Object to compare.</param>
		/// <returns>True if objects are the same, false if not.</returns>
		public override bool Equals(object obj)
		{
			bool retValue = false;
			if (obj != null)
			{
				if (obj.GetType() == typeof(PendingSet))
				{
					PendingSet objCompare = (PendingSet)obj;
					if (objCompare.TestSetId == this.TestSetId)
						retValue = true;
				}
			}
			return retValue;
		}

		/// <summary>Gets the unique hashcode for the object.</summary>
		/// <returns></returns>
		public override int GetHashCode()
		{
			return this._testSetId.GetHashCode();
		}
		#endregion

		#region Properties
		/// <summary>The Test Set ID of this PendingSet.</summary>
		public int TestSetId
		{
			get
			{
				return this._testSetId;
			}
		}

		/// <summary>The scheduled date for the PendingSet.</summary>
		public DateTime? ScheduledDate
		{
			get;
			set;
		}

		/// <summary>The name of the Test Run.</summary>
		public string Name
		{
			get
			{
				return this._name;
			}
		}

		/// <summary>The number of test runs in the PendingSet.</summary>
		public int Count
		{
			get
			{
				return this._testRuns.Count();
			}
		}

		/// <summary>Whether the scheduled testrun is over due for execurtion or not.</summary>
		public bool OverDue
		{
			get;
			set;
		}

		/// <summary>The current status of the pendingset.</summary>
		public PendingSetRunStatusEnum Status
		{
			get
			{
				return this._status;
			}
			set
			{
				this._status = value;
			}
		}

		/// <summary>The projectid of the test set.</summary>
		public int ProjectId
		{
			get
			{
				return this._projectId;
			}
		}
		#endregion

		#region Static Methods
		/// <summary>Compares two PendingSet objects to determine if they're equal.</summary>
		/// <param name="obj1">The first PendingSet to compare.</param>
		/// <param name="obj2">The second PendingSet to compare.</param>
		public static bool IsEqual(PendingSet obj1, PendingSet obj2)
		{
			bool retValue = false;

			if ((obj1 != null) && (obj2 != null))
			{
				if (obj1.TestSetId == obj2.TestSetId)
					retValue = true;
			}

			return retValue;
		}

		/// <summary>Enumeration of the pending testrun.</summary>
		public enum PendingSetRunStatusEnum : int
		{
			NotStarted = 1,
			InProgress = 2,
			Completed = 3,
			Blocked = 4,
			Deferred = 5
		}
		#endregion

		/// <summary>The list of test runs in this test set.</summary>
		public List<RemoteAutomatedTestRun> TestRuns
		{
			get
			{
				return this._testRuns;
			}
		}

		/// <summary>Gets the indexed test run.</summary>
		/// <param name="index">The index of the test run to retrieve.</param>
		/// <returns>An RemoteAutomatedTestRun</returns>
		public RemoteAutomatedTestRun this[int index]
		{
			get
			{
				return this._testRuns[index];
			}
		}

        /// <summary>
        /// Returns the display name of the pending set for use in a WinForm list box
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            if (this.ScheduledDate.HasValue)
            {
                return "TX" + this._testSetId + ": " + this._name + " - " + this._status.ToString() + " - Due: " + this.ScheduledDate.Value.ToShortTimeString();
            }
            else
            {
                return "TX" + this._testSetId + ": " + this._name + " - " + this._status.ToString();
            }
        }

		#region IComparable Members
		/// <summary>Compares the date of one PendingSet to another.</summary>
		/// <param name="obj">The date to compare it to.</param>
		/// <returns>Comapritor.</returns>
		int IComparable.CompareTo(object obj)
		{
			if (obj.GetType() == typeof(PendingSet))
			{
				return this.ScheduledDate.Value.CompareTo(((PendingSet)obj).ScheduledDate);
			}
			else
			{
				throw new Exception("Cannot compare non PendingSet type.");
			}
		}
		#endregion
	}
}

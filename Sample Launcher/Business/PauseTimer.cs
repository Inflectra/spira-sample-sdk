using System;
using System.Threading;
using System.Diagnostics;

namespace Inflectra.Rapise.RapiseLauncher.Business
{
	[DebuggerStepThrough]
	public class PauseTimer
	{
		public event EventHandler Elapsed;
		private System.Threading.Timer _timer;
		private bool _enabled = false;
		private int _timePassed;
		private DateTime _lastStart;
		private object _locker;
		private int _interval;

		/// <summary>Creates a new instance of the PauseTimer.</summary>
		public PauseTimer()
		{
			this._timer = new System.Threading.Timer(callback);
			this._locker = new object();

			this.Active = false;
		}

		/// <summary>Creates a new instance of the PauseTimer.</summary>
		/// <param name="interval">The interval in milliseconds to call the Elapsed event.</param>
		public PauseTimer(int interval)
		{
			this._timer = new System.Threading.Timer(callback);
			this._locker = new object();

			this.Interval = interval;
			this.Active = false;
		}

		/// <summary>The amount in milliseconds to call the Elapsed event.</summary>
		public int Interval
		{
			get
			{
				return this._interval;
			}
			set
			{
				//Reset timer.
				this.Reset();
				//Set new interval.
				this._interval = value;
			}
		}

		/// <summary>Sets whether the timer is running or not (paused).</summary>
		public bool Active
		{
			get
			{
				return this._enabled;
			}
			set
			{
				lock (this._locker)
				{
					if (value == this._enabled)
						return;

					this._enabled = value;

					if (value)
					{
						this._lastStart = DateTime.Now;
						this._timer.Change(this.Interval - this._timePassed, this.Interval);
					}
					else
					{
						this._timer.Change(Timeout.Infinite, Timeout.Infinite);
						//this._timer.Change(0, 0);
						this._timePassed += Math.Min(this.Interval, (int)(DateTime.Now - this._lastStart).TotalMilliseconds); //If we're over, make it the Interval.
						this._timePassed = Math.Min(this._timePassed, this.Interval); //If we're over, make it the Interval.
					}
				}
			}
		}

		/// <summary>Returns the amount or time in milliseconds left.</summary>
		public int TimeLeft
		{
			get
			{
				if (!this.Active)
				{
					if (this._timePassed > 0)
						return (this.Interval - this._timePassed);
					else
						return this._interval;
				}
				else
					return (this.Interval - ((int)(DateTime.Now - this._lastStart).TotalMilliseconds + this._timePassed));
			}
		}

		/// <summary>Resets the timer, causing the next Active call to start the counter over.</summary>
		public void Reset()
		{
			lock (this._locker)
			{
				this.Active = false;
				this._timePassed = 0;
			}
		}

		/// <summary>Dummy function to raise events.</summary>
		/// <param name="dummy">Object</param>
		private void callback(object dummy)
		{
			bool fire;
			lock (this._locker)
			{
				this._lastStart = DateTime.Now;
				this._timePassed = 0;
				fire = this.Elapsed != null && this._enabled;
			}
			try
			{
				// System.Timers.Timer.Elapsed swallows exceptions, bah
				if (fire)
					this.Elapsed(this, EventArgs.Empty);
			}
			catch { }
		}
	}
}

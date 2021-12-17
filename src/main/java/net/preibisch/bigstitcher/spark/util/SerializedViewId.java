package net.preibisch.bigstitcher.spark.util;

import java.io.Serializable;

import mpicbg.spim.data.sequence.ViewId;

public class SerializedViewId extends ViewId implements Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2607372542265476560L;

	/**
	 * The timepoint id.
	 */
	protected int timepoint;

	/**
	 * The setup id.
	 */
	protected int setup;

	public SerializedViewId( final int timepointId, final int setupId )
	{
		super(timepointId,setupId);
		timepoint = timepointId;
		setup = setupId;
	}

	/**
	 * Get the timepoint id.
	 *
	 * @return timepoint id
	 */
	public int getTimePointId()
	{
		return timepoint;
	}

	void setTimePointId( final int id )
	{
		timepoint = id;
	}

	/**
	 * Get the setup id.
	 *
	 * @return setup id
	 */
	public int getViewSetupId()
	{
		return setup;
	}

	void setViewSetupId( final int id )
	{
		setup = id;
	}

	/**
	 * Two {@link ViewId} are equal if they have the same
	 * {@link #getTimePointId() timepoint} and {@link #getViewSetupId() setup}
	 * ids.
	 */
	@Override
	public boolean equals( final Object o )
	{
		if ( o == null )
		{
			return false;
		}
		else if ( o instanceof ViewId )
		{
			final ViewId i = ( ViewId ) o;

			if ( i.getTimePointId() == getTimePointId() && i.getViewSetupId() == getViewSetupId() )
				return true;
			else
				return false;
		}
		else
		{
			return false;
		}
	}

	/**
	 * Order by {@link #getTimePointId() timepoint} id, then
	 * {@link #getViewSetupId() setup} id.
	 */
	@Override
	public int compareTo( final ViewId o )
	{
		if ( timepoint == o.getTimePointId() )
			return setup - o.getViewSetupId();
		else
			return timepoint - o.getTimePointId();
	}

	@Override
	public int hashCode()
	{
		// some non-colliding hash assuming we have not more that 100000 viewsetups
		return getViewSetupId() + getTimePointId() * 100000;
	}
}

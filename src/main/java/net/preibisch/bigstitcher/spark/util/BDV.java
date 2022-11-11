package net.preibisch.bigstitcher.spark.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Writer;

import bdv.img.n5.N5ImageLoader;
import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.XmlIoSpimData;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewRegistrations;
import mpicbg.spim.data.sequence.Angle;
import mpicbg.spim.data.sequence.Channel;
import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import mpicbg.spim.data.sequence.Illumination;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.Tile;
import mpicbg.spim.data.sequence.TimePoint;
import mpicbg.spim.data.sequence.TimePoints;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.util.Intervals;
import net.preibisch.bigstitcher.spark.AffineFusion.StorageType;

public class BDV {

	public static void writeBDVMetaData(
			final N5Writer driverVolumeWriter,
			final StorageType storageType,
			final DataType dataType,
			final long[] dimensions,
			final Compression compression,
			final int[] blockSize,
			final String bdvString,
			final String n5Path,
			final String xmlOutPathString,
			final String angleIds,
			final String illuminationIds, 
			final String channelIds,
			final String tileIds ) throws SpimDataException, IOException
	{
		System.out.println( "Writing BDV-metadata ... " );

		final ViewId viewId = Import.getViewId( bdvString );

		//final String xmlPath = null;
		if ( StorageType.N5.equals(storageType) )
		{
			final File xmlOutPath;
			if ( xmlOutPathString == null )
				xmlOutPath = new File( new File( n5Path ).getParent(), "dataset.xml" );
			else
				xmlOutPath = new File( xmlOutPathString );

			System.out.println( "XML: " + xmlOutPath.getAbsolutePath() );

			if ( xmlOutPath.exists() )
			{
				System.out.println( "XML exists. Parsing and adding.");
				final XmlIoSpimData io = new XmlIoSpimData();
				final SpimData spimData = io.load( xmlOutPath.getAbsolutePath() );

				boolean tpExists = false;
				boolean viewSetupExists = false;

				for ( final ViewDescription viewId2 : spimData.getSequenceDescription().getViewDescriptions().values() )
				{
					if ( viewId2.equals( viewId ) )
					{
						System.out.println( "ViewId you specified already exists in the XML, cannot continue." );
						System.exit( 0 );
					}

					if ( viewId2.getTimePointId() == viewId.getTimePointId() )
						tpExists = true;

					if ( viewId2.getViewSetupId() == viewId.getViewSetupId() )
					{
						viewSetupExists = true;

						// dimensions have to match
						if ( !Intervals.equalDimensions( new FinalDimensions( dimensions ), viewId2.getViewSetup().getSize() ) )
						{
							System.out.println( "ViewSetup you specified already exists in the XML, but with different dimensions, cannot continue." );
							System.exit( 0 );
						}
					}
				}

				final List<ViewSetup> setups = new ArrayList<>( spimData.getSequenceDescription().getViewSetups().values() );

				if ( !viewSetupExists )
				{
					final Iterator<ViewSetup> i = setups.iterator();
					ViewSetup tmp = i.next();

					Channel c0 = tmp.getChannel();
					Angle a0 = tmp.getAngle();
					Illumination i0 = tmp.getIllumination();
					Tile t0 = tmp.getTile();

					while ( i.hasNext() )
					{
						tmp = i.next();
						if ( tmp.getChannel().getId() > c0.getId() )
							c0 = tmp.getChannel();
						if ( tmp.getAngle().getId() > a0.getId() )
							a0 = tmp.getAngle();
						if ( tmp.getIllumination().getId() > i0.getId() )
							i0 = tmp.getIllumination();
						if ( tmp.getTile().getId() > t0.getId() )
							t0 = tmp.getTile();
					}

					if ( angleIds != null )
						a0 = new Angle( a0.getId() + 1 );
					if ( illuminationIds != null )
						i0 = new Illumination( i0.getId() + 1 );
					if ( tileIds != null )
						t0 = new Tile( t0.getId() + 1 );
					if ( tileIds != null || ( angleIds == null && illuminationIds == null && tileIds == null && tpExists ) ) // nothing was defined, then increase channel
						c0 = new Channel( c0.getId() + 1 );

					final Dimensions d0 = new FinalDimensions( dimensions );
					final VoxelDimensions vd0 = new FinalVoxelDimensions( "px", 1, 1, 1 );

					setups.add( new ViewSetup( viewId.getViewSetupId(), "setup " + viewId.getViewSetupId(), d0, vd0, t0, c0, a0, i0 ) );
				}

				final TimePoints timepoints;
				if ( !tpExists )
				{
					final List<TimePoint> tps = spimData.getSequenceDescription().getTimePoints().getTimePointsOrdered();
					tps.add( new TimePoint( viewId.getTimePointId() ) );
					timepoints = new TimePoints( tps );
				}
				else
				{
					timepoints = spimData.getSequenceDescription().getTimePoints();
				}

				final Map<ViewId, ViewRegistration> registrations = spimData.getViewRegistrations().getViewRegistrations();
				registrations.put( viewId, new ViewRegistration( viewId.getTimePointId(), viewId.getViewSetupId() ) );
				final ViewRegistrations viewRegistrations = new ViewRegistrations( registrations );

				final SequenceDescription sequence = new SequenceDescription(timepoints, setups, null);
				sequence.setImgLoader( new N5ImageLoader( new File( n5Path ), sequence) );

				final SpimData spimDataNew = new SpimData( xmlOutPath.getParentFile(), sequence, viewRegistrations);
				new XmlIoSpimData().save( spimDataNew, xmlOutPath.getAbsolutePath() );
			}
			else
			{
				System.out.println( "New XML.");

				final ArrayList< ViewSetup > setups = new ArrayList<>();

				final Channel c0 = new Channel( 0 );
				final Angle a0 = new Angle( 0 );
				final Illumination i0 = new Illumination( 0 );
				final Tile t0 = new Tile( 0 );

				final Dimensions d0 = new FinalDimensions( dimensions );
				final VoxelDimensions vd0 = new FinalVoxelDimensions( "px", 1, 1, 1 );
				setups.add( new ViewSetup( viewId.getViewSetupId(), "setup " + viewId.getViewSetupId(), d0, vd0, t0, c0, a0, i0 ) );

				final ArrayList< TimePoint > tps = new ArrayList<>();
				tps.add( new TimePoint( viewId.getTimePointId() ) );
				final TimePoints timepoints = new TimePoints( tps );

				final HashMap< ViewId, ViewRegistration > registrations = new HashMap<>();
				registrations.put( viewId, new ViewRegistration( viewId.getTimePointId(), viewId.getViewSetupId() ) );
				final ViewRegistrations viewRegistrations = new ViewRegistrations( registrations );

				final SequenceDescription sequence = new SequenceDescription(timepoints, setups, null);
				sequence.setImgLoader( new N5ImageLoader( new File( n5Path ), sequence) );

				final SpimData spimData = new SpimData( xmlOutPath.getParentFile(), sequence, viewRegistrations);

				new XmlIoSpimData().save( spimData, xmlOutPath.getAbsolutePath() );
			}

			// set N5 attributes for setup
			// e.g. {"compression":{"type":"gzip","useZlib":false,"level":1},"downsamplingFactors":[[1,1,1],[2,2,1]],"blockSize":[128,128,32],"dataType":"uint16","dimensions":[512,512,86]}
			String ds = "setup" + viewId.getViewSetupId();
			System.out.println( "setting attributes for '" + "setup" + viewId.getViewSetupId() + "'");
			driverVolumeWriter.setAttribute(ds, "dataType", dataType );
			driverVolumeWriter.setAttribute(ds, "blockSize", blockSize );
			driverVolumeWriter.setAttribute(ds, "dimensions", dimensions );
			driverVolumeWriter.setAttribute(ds, "compression", compression );
			driverVolumeWriter.setAttribute(ds, "downsamplingFactors", new int[][] {{1,1,1}} );

			// set N5 attributes for timepoint
			// e.g. {"resolution":[1.0,1.0,3.0],"saved_completely":true,"multiScale":true}
			ds ="setup" + viewId.getViewSetupId() + "/" + "timepoint" + viewId.getTimePointId();
			driverVolumeWriter.setAttribute(ds, "resolution", new double[] {1,1,1} );
			driverVolumeWriter.setAttribute(ds, "saved_completely", true );
			driverVolumeWriter.setAttribute(ds, "multiScale", false );

			// set additional N5 attributes for s0 dataset
			ds = ds + "/s0";
			driverVolumeWriter.setAttribute(ds, "downsamplingFactors", new int[] {1,1,1} );

		}
		else if ( StorageType.HDF5.equals(storageType) )
		{
			System.out.println( "BDV-compatible dataset cannot be written for " + storageType + " (yet).");
			System.exit( 0 );
		}
		else
		{
			System.out.println( "BDV-compatible dataset cannot be written for " + storageType + " (yet).");
			System.exit( 0 );
		}
	}
}

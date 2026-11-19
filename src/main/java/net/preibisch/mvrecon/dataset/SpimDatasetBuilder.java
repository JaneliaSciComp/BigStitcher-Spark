package net.preibisch.mvrecon.dataset;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import loci.formats.IFormatReader;
import loci.formats.meta.MetadataRetrieve;
import mpicbg.spim.data.generic.sequence.BasicViewDescription;
import mpicbg.spim.data.registration.ViewRegistrations;
import mpicbg.spim.data.sequence.Angle;
import mpicbg.spim.data.sequence.Channel;
import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import mpicbg.spim.data.sequence.Illumination;
import mpicbg.spim.data.sequence.ImgLoader;
import mpicbg.spim.data.sequence.MissingViews;
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
import net.preibisch.mvrecon.fiji.datasetmanager.DatasetCreationUtils;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBoxes;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.FileMapImgLoaderLOCI;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.LegacyStackImgLoaderLOCI;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.filemap2.CZIImageLoader;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.filemap2.FileMapEntry;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.filemap2.FileMapImgLoaderLOCI2;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.util.BioformatsReaderUtils;
import net.preibisch.mvrecon.fiji.spimdata.intensityadjust.IntensityAdjustments;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.pointspreadfunctions.PointSpreadFunctions;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.StitchingResults;
import ome.units.UNITS;
import ome.units.quantity.Length;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

public class SpimDatasetBuilder {

	static class ViewIndex {
		final int tp, ch, il, ang;

		ViewIndex(int tp, int ch, int il, int ang) {
			this.tp = tp;
			this.ch = ch;
			this.il = il;
			this.ang = ang;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null || getClass() != o.getClass()) return false;
			ViewIndex tileIndex = (ViewIndex) o;
			return tp == tileIndex.tp && ch == tileIndex.ch && il == tileIndex.il && ang == tileIndex.ang;
		}

		@Override
		public int hashCode() {
			return Objects.hash(tp, ch, il, ang);
		}

		@Override
		public String toString() {
			return new ToStringBuilder(this)
					.append("tp", tp)
					.append("ch", ch)
					.append("il", il)
					.append("ang", ang)
					.toString();
		}
	}

	static class StackFile {
		final ViewIndex viewIndex;
		final int ti;
		final URI baseURI;
		final String relativeFilePath;
		int nImages = -1;
		int nTp = -1;
		int nCh = -1;
		int sizeZ = -1;
		int sizeY = -1;
		int sizeX = -1;

		StackFile(int tp, int ch, int il, int ang, int ti, URI baseURI, String relativeFilePath )
		{
			this.viewIndex = new ViewIndex(tp, ch, il, ang);
			this.ti = ti;
			this.baseURI = baseURI;
			this.relativeFilePath = relativeFilePath;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null || getClass() != o.getClass()) return false;
			StackFile stackFile = (StackFile) o;
			return ti == stackFile.ti &&
					Objects.equals(viewIndex, stackFile.viewIndex) &&
					Objects.equals(baseURI, stackFile.baseURI) &&
					Objects.equals(relativeFilePath, stackFile.relativeFilePath);
		}

		@Override
		public int hashCode() {
			return Objects.hash(viewIndex, ti, baseURI, relativeFilePath);
		}

		@Override
		public String toString() {
			return new ToStringBuilder(this)
					.append("view", viewIndex)
					.append("ti", ti)
					.append("baseURI", baseURI)
					.append("relativeFilePath", relativeFilePath)
					.toString();
		}

		Path getFilePath() {
			return Paths.get(baseURI).resolve(relativeFilePath);
		}

		int getTp() {
			return viewIndex.tp;
		}

		int getTi() {
			return ti;
		}

		public int getCh() {
			return viewIndex.ch;
		}

		public int getAng() {
			return viewIndex.ang;
		}

		public int getIl() {
			return viewIndex.il;
		}
	}

	interface ViewSetupBuilder {
		SequenceDescription getSequenceDescription();
		ViewSetupBuilder setImgLoader();
		ViewSetupBuilder createViewSetups(List<StackFile> stackFiles);
	}


	/**
	 * LOCIViewSetupBuilder is based on LegacyStackImgLoaderLOCI from multiview-reconstruction library
	 */
	static class LOCIViewSetupBuilder implements ViewSetupBuilder {

		private final SequenceDescription sequenceDescription;
		private final Map<Integer, StackFile> viewToStackFileMap = new HashMap<>();

		LOCIViewSetupBuilder(TimePoints timePoints) {
			// the virtual loaders look up present views through the missing views, which must not be null
			this.sequenceDescription = new SequenceDescription(
					timePoints,
					/*view setups*/Collections.emptyList(),
					/*img loader*/null,
					new MissingViews(Collections.emptyList())
			);
		}

		@Override
		public SequenceDescription getSequenceDescription() {
			return sequenceDescription;
		}

		@Override
		public LOCIViewSetupBuilder setImgLoader() {
			Map<BasicViewDescription< ? >, FileMapEntry> fileMap = new HashMap<>();
			for (ViewSetup vs : sequenceDescription.getViewSetupsOrdered()) {
				StackFile stackFile = viewToStackFileMap.get(vs.getId());
				ViewDescription vdI = sequenceDescription.getViewDescription( stackFile.getTp(), vs.getId() );
				FileMapEntry fileMapEntry = new FileMapEntry(
						stackFile.getFilePath().toFile(),
						vs.getTile().getId() - (stackFile.getTi() * stackFile.nImages), // recreate the image index within the file
						vs.getChannel().getId()-stackFile.getCh());
				fileMap.put( vdI,  fileMapEntry);
			}

			sequenceDescription.setImgLoader(createImageLoader(fileMap));
			return this;
		}

		protected ImgLoader createImageLoader(Map<BasicViewDescription< ? >, FileMapEntry> fileMap) {
			return new FileMapImgLoaderLOCI(
					fileMap,
					sequenceDescription,
					false
			);
		}

		@SuppressWarnings("unchecked")
		@Override
		public LOCIViewSetupBuilder createViewSetups(List<StackFile> stackFiles) {
			int nfiles = stackFiles.size();
			for (int sfi = 0; sfi < nfiles; sfi++) {
				StackFile stackFile = stackFiles.get(sfi);
				File tileFile = stackFile.getFilePath().toFile();
				if ( !tileFile.exists() )
				{
					continue;
				}

				IFormatReader formatReader = BioformatsReaderUtils.createImageReaderWithSetupHooks();
				try {
					if ( !LegacyStackImgLoaderLOCI.createOMEXMLMetadata( formatReader ) ) {
						try {
							formatReader.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
						continue;
					}

					formatReader.setId( tileFile.toString() );
					MetadataRetrieve retrieve = (MetadataRetrieve)formatReader.getMetadataStore();

					int seriesCount = formatReader.getSeriesCount();
					Map<String, Object> globalMetadata = formatReader.getGlobalMetadata();

					for (int series = 0; series < seriesCount; series++) {
						formatReader.setSeries(series);
						stackFile.nImages = seriesCount;
						stackFile.nTp = formatReader.getSizeT();
						stackFile.nCh = formatReader.getSizeC();
						stackFile.sizeZ = formatReader.getSizeZ();
						stackFile.sizeY = formatReader.getSizeY();
						stackFile.sizeX = formatReader.getSizeX();

						double[] tileLocation = getTileLocation(retrieve, globalMetadata, series);
						Length resX = retrieve.getPixelsPhysicalSizeX(series);
						Length resY = retrieve.getPixelsPhysicalSizeY(series);
						Length resZ = retrieve.getPixelsPhysicalSizeZ(series);

						double rX = resX != null ? resX.value(UNITS.MICROMETER).doubleValue() : 1;
						double rY = resY != null ? resY.value(UNITS.MICROMETER).doubleValue() : 1;
						double rZ = resZ != null ? resZ.value(UNITS.MICROMETER).doubleValue() : 1;

						VoxelDimensions voxelDimensions = new FinalVoxelDimensions("um", rX, rY, rZ);
						System.out.println("Voxel dimensions: " + voxelDimensions);
						int metadataChannels = retrieve.getChannelCount(series);

						for (int metadataChIndex = 0; metadataChIndex < metadataChannels; metadataChIndex++) {
							String metadataChName = retrieve.getChannelName(series, metadataChIndex);
							String chName;
							if (StringUtils.isEmpty(metadataChName)) {
								chName = String.valueOf(stackFile.getCh());
							} else {
								chName  = metadataChName;
							}
							// currently viewIndex is only based on the number of images and channels
							// but a correct implementation would also consider timepoints, illuminations and angles
							// for now I am ignoring them because so far we never needed them.
							int viewIndex = metadataChIndex * nfiles * seriesCount + sfi * seriesCount + series;
							Tile tile = new Tile(stackFile.getTi() * seriesCount + series);
							tile.setLocation(tileLocation);
							Channel channel = new Channel(stackFile.getCh() + metadataChIndex, chName);
							ViewSetup vs = new ViewSetup(
									viewIndex,
									String.valueOf(viewIndex),
									new FinalDimensions(stackFile.sizeX, stackFile.sizeY, stackFile.sizeZ),
									voxelDimensions,
									tile,
									channel,
									new Angle(stackFile.getAng()),
									new Illumination(stackFile.getIl())
							);
							viewToStackFileMap.put(viewIndex, stackFile);
							((Map<Integer, ViewSetup>) sequenceDescription.getViewSetups()).put(viewIndex, vs);
						}
					}
				} catch (Exception e) {
					throw new IllegalStateException("Could not read " + stackFile, e);
				}
			}
			return this;
		}

		/**
		 * Location of the tile (x, y, z in um) of the given series, set on the Tile of every view of the series.
		 */
		protected double[] getTileLocation(MetadataRetrieve metadataRetrieve, Map<String, Object> globalMetadata, int series) {
			return new double[]{
					getOffsetX(metadataRetrieve, globalMetadata, series),
					getOffsetY(metadataRetrieve, globalMetadata, series),
					getOffsetZ(metadataRetrieve, globalMetadata, series)
			};
		}

		private double getOffsetX(MetadataRetrieve metadataRetrieve, Map<String, Object> globalMetadata, int series) {
			// get position from global metadata (image number in metadata is 1-based in global metadata)
			Double val = findPositionInGlobalMeta(
					globalMetadata,
					series + 1,
					Arrays.asList("PositionX", "Position|X"));

			if (val != null) {
				return val;
			}

			try {
				Length l = metadataRetrieve.getStageLabelX(series);
				return l != null && l.value(UNITS.MICROMETER) != null ? l.value(UNITS.MICROMETER).doubleValue() : 0;
			} catch (NullPointerException e) {
				return 0;
			}
		}

		private double getOffsetY(MetadataRetrieve metadataRetrieve, Map<String, Object> globalMetadata, int series) {
			// get position from global metadata (image number in metadata is 1-based in global metadata)
			Double val = findPositionInGlobalMeta(
					globalMetadata,
					series + 1,
					Arrays.asList("PositionY", "Position|Y"));

			if (val != null) {
				return val;
			}

			try {
				Length l = metadataRetrieve.getStageLabelY(series);
				return l != null && l.value(UNITS.MICROMETER) != null ? l.value(UNITS.MICROMETER).doubleValue() : 0;
			} catch (NullPointerException e) {
				return 0;
			}
		}

		private double getOffsetZ(MetadataRetrieve metadataRetrieve, Map<String, Object> globalMetadata, int series) {
			// get position from global metadata (image number in metadata is 1-based in global metadata)
			Double val = findPositionInGlobalMeta(
					globalMetadata,
					series + 1,
					Arrays.asList("PositionZ", "Position|Z"));

			if (val != null) {
				return val;
			}

			try {
				Length l = metadataRetrieve.getStageLabelZ(series);
				return l != null && l.value(UNITS.MICROMETER) != null ? l.value(UNITS.MICROMETER).doubleValue() : 0;
			} catch (NullPointerException e) {
				return 0;
			}
		}

		private Double findPositionInGlobalMeta(Map<String, Object> globalMeta, int imageNumber, List<String> patterns) {
			// Preferred key patterns
			String[] prefixes = {
					"Information|Image|V|View|",
					"Information|Image|S|Scene|",
					""
			};
			for (String prefix : prefixes) {
				for (String pattern : patterns) {
					String searchPattern = prefix + pattern;
					for (String key : globalMeta.keySet()) {
						if (key.contains(searchPattern) && key.contains("#")) {
							int imageNumberFromKey = extractImageNumberFromKey(key);
							if (imageNumberFromKey == imageNumber) {
								Object val = globalMeta.get(key);
								Double dval = null;
								if (val instanceof Double) {
									dval = (Double) val;
								} else if (val instanceof String) {
									dval = Double.parseDouble((String) val);
								}
								if (dval != null) {
									// Convert from meters to micrometers if needed
									if (Math.abs(dval) < 1 && Math.abs(dval) > 0)
										dval *= 1e6;
									return dval;
								}
							}
						}
					}
				}
			}
			return null;
		}

		private int extractImageNumberFromKey(String key) {
			int hashIdx = key.lastIndexOf('#');
			if (hashIdx >= 0 && hashIdx < key.length() - 1) {
				try {
					return Integer.parseInt(key.substring(hashIdx + 1).trim());
				} catch (NumberFormatException e) {
					return -1;
				}
			}
			return -1;
		}
	}


	static class CZIViewSetupBuilder extends LOCIViewSetupBuilder {

		public CZIViewSetupBuilder(TimePoints timePoints) {
			super(timePoints);
		}

		@Override
		protected ImgLoader createImageLoader(Map<BasicViewDescription<?>, FileMapEntry> fileMap) {
			return new CZIImageLoader(fileMap, getSequenceDescription());
		}
	}

	/**
	 * Nikon ND2 files: every series is one XY stage position (tile) with all its channels.
	 * ND2Reader stores the stage position as OME plane positions only (no stage labels and no
	 * Position|X style global keys), so the location of a tile is the position of its first plane.
	 * The images are read plane by plane on demand (FileMapImgLoaderLOCI2), an ND2 holds hundreds of GB.
	 */
	static class ND2ViewSetupBuilder extends LOCIViewSetupBuilder {

		ND2ViewSetupBuilder(TimePoints timePoints) {
			super(timePoints);
		}

		@Override
		protected double[] getTileLocation(MetadataRetrieve metadataRetrieve, Map<String, Object> globalMetadata, int series) {
			// the stage coordinates are taken as they are; the axis convention (mvr's LegacyStackImgLoaderLOCI
			// negates x and y of ND2 plane positions) still has to be checked against the image content
			double[] location = new double[3];
			if (metadataRetrieve.getPlaneCount(series) == 0) {
				return location;
			}
			Length[] position = {
					metadataRetrieve.getPlanePositionX(series, 0),
					metadataRetrieve.getPlanePositionY(series, 0),
					metadataRetrieve.getPlanePositionZ(series, 0)
			};
			for (int d = 0; d < 3; d++) {
				if (position[d] != null && position[d].value(UNITS.MICROMETER) != null) {
					location[d] = position[d].value(UNITS.MICROMETER).doubleValue();
				}
			}
			return location;
		}

		@Override
		protected ImgLoader createImageLoader(Map<BasicViewDescription<?>, FileMapEntry> fileMap) {
			return new FileMapImgLoaderLOCI2(fileMap, getSequenceDescription());
		}

		@Override
		public ND2ViewSetupBuilder createViewSetups(List<StackFile> stackFiles) {
			super.createViewSetups(stackFiles);
			// timepoints inside a file are not mapped to timepoints of the dataset (yet)
			for (StackFile stackFile : stackFiles) {
				if (stackFile.nTp > 1) {
					throw new IllegalArgumentException(stackFile.getFilePath() + " has " + stackFile.nTp +
							" timepoints; only ND2 files with a single timepoint are supported");
				}
			}
			return this;
		}
	}

	static class N5MultichannelViewSetupBuilder implements ViewSetupBuilder {

		private final URI n5ContainerURI;
		private final SequenceDescription sequenceDescription;
		private final Map<ViewId, String> viewIdToPath;
		private final N5Reader n5Reader;
		private final N5MultichannelProperties n5MultichannelProperties;

		public N5MultichannelViewSetupBuilder(URI n5ContainerURI, TimePoints timePoints) {
			this.n5ContainerURI = n5ContainerURI;
			this.sequenceDescription = new SequenceDescription(
					timePoints,
					/*view setups*/Collections.emptyList()
			);
			this.viewIdToPath = new HashMap<>();
			n5Reader = new N5FSReader(n5ContainerURI.toString());
			n5MultichannelProperties = new N5MultichannelProperties(sequenceDescription, viewIdToPath);
		}

		@Override
		public SequenceDescription getSequenceDescription() {
			return sequenceDescription;
		}

		@Override
		public N5MultichannelViewSetupBuilder setImgLoader() {
			sequenceDescription.setImgLoader(
					new N5MultichannelLoader( n5ContainerURI, StorageFormat.N5, sequenceDescription, viewIdToPath )
			);
			return this;
		}

		@SuppressWarnings("unchecked")
		@Override
		public N5MultichannelViewSetupBuilder createViewSetups(List<StackFile> stackFiles) {
			for (int i = 0; i < stackFiles.size(); i++) {
				StackFile stackFile = stackFiles.get(i);
				if ( Files.notExists(stackFile.getFilePath()) )
				{
					continue;
				}
				viewIdToPath.put(new ViewId(stackFile.getTp(), i), stackFile.relativeFilePath);

				Map<String, Object> pixelResolutions = n5MultichannelProperties.getRootAttribute(n5Reader, "pixelResolution", Map.class);
				VoxelDimensions voxelDimensions;
				if (pixelResolutions != null) {
					double[] res = ((List<Double>) pixelResolutions.getOrDefault("dimensions", Arrays.asList(1., 1., 1.)))
							.stream()
							.mapToDouble(d -> d)
							.toArray();
					String resUnits = (String) pixelResolutions.getOrDefault("unit", "voxel");
					voxelDimensions = new FinalVoxelDimensions(resUnits, res);
				} else {
					voxelDimensions = new FinalVoxelDimensions("voxel", 1., 1., 1.);
				}
				System.out.println("Voxel dimensions: " + voxelDimensions);
				long[] dims = n5MultichannelProperties.getDimensions(n5Reader, i, stackFile.getTp(), 0);
				Dimensions size = new FinalDimensions(dims[0], dims[1], dims[2]);
				ViewSetup vs = new ViewSetup(
						i, // in this case view index coincides with stack file index
						stackFile.relativeFilePath,
						size,
						voxelDimensions,
						new Tile(stackFile.getTi()),
						new Channel(stackFile.getCh()),
						new Angle(stackFile.getAng()),
						new Illumination(stackFile.getIl())
				);
				((Map<Integer, ViewSetup>) sequenceDescription.getViewSetups()).put(i, vs);
			}

			return this;
		}

	}

	static class StackPattern {
		final String sourcePattern;
		// globPattern is used for file lookup
		final String globPattern;
		// regexPattern is used for creating tile metadata based on t|c|i|a|x
		final Pattern regexPattern;
		final Set<String> keys;

		StackPattern( String sourcePattern ) {
			this.sourcePattern = sourcePattern;
			this.globPattern = sourcePattern
					.replaceAll("\\{t\\}", "*")
					.replaceAll("\\{c\\}", "*")
					.replaceAll("\\{i\\}", "*")
					.replaceAll("\\{a\\}", "*")
					.replaceAll("\\{x\\}", "*");
			this.regexPattern = Pattern.compile( sourcePattern
					.replaceAll("\\.", "\\\\.") // escape dot
					.replaceAll("\\*", ".*")
					.replaceAll("\\{t\\}", "(?<tp>\\\\D*?\\\\d+)")    // timepoint
					.replaceAll("\\{c\\}", "(?<ch>\\\\D*?\\\\d+)")    // channel
					.replaceAll("\\{i\\}", "(?<il>\\\\D*?\\\\d+)")    // illumination
					.replaceAll("\\{a\\}", "(?<ang>\\\\D*?\\\\d+)")   // angle
					.replaceAll("\\{x\\}", "(?<ti>\\\\D*?\\\\d+)") ); // tile

			this.keys = initializeKeys(sourcePattern);
		}

		private Set<String> initializeKeys(String sourcePattern) {
			Set<String> patternKeys = new HashSet<>();
			String regexStr = Pattern.quote(sourcePattern);
			Matcher m = Pattern.compile("\\{(t|c|i|a|x)\\}").matcher(regexStr);
			while (m.find()) {
				String key = m.group(1);
				patternKeys.add(key);
			}
			return patternKeys;
		}

		String getGlobPattern() {
			return "glob:" + globPattern;
		}

		int getSearchDepth() {
			return (int) sourcePattern.chars().filter(c -> c == File.separatorChar).count();
		}

		boolean hasKey(String key) {
			return keys.contains(key);
		}
	}

	private final StackPattern fileNamePattern;
	private boolean swapXY = false;
	private boolean flipX = false;
	private boolean flipY = false;

	public SpimDatasetBuilder(String fileNamePattern )
	{
		this.fileNamePattern = new StackPattern(fileNamePattern);
	}

	/**
	 * Map the stage positions read from the image metadata (the tile locations) onto the image axes,
	 * for microscopes whose stage axes differ from the camera's: swapXY exchanges x and y first,
	 * then flipX / flipY negate x / y of the result. Together they give all eight orientations,
	 * e.g. swapXY + flipX + flipY maps (x, y) to (-y, -x).
	 */
	public SpimDatasetBuilder transformStagePositions(boolean swapXY, boolean flipX, boolean flipY)
	{
		this.swapXY = swapXY;
		this.flipX = flipX;
		this.flipY = flipY;
		return this;
	}

	public SpimData2 createDataset(URI imagePath) {
		List<StackFile> stackFiles = getStackFiles(imagePath);

		// collect timepoints from stack files
		Set<TimePoint> timePoints = stackFiles.stream()
				.map(si -> new TimePoint(si.viewIndex.tp))
				.collect(Collectors.toSet());

		SequenceDescription sequenceDescription = createViewSetupBuilder(imagePath, stackFiles, new TimePoints(timePoints))
				.createViewSetups(stackFiles)
				.setImgLoader()
				.getSequenceDescription();

		transformTileLocations(sequenceDescription);

		// get the min resolution from all calibrations
		double minResolution = DatasetCreationUtils.minResolution(
				sequenceDescription,
				sequenceDescription.getViewDescriptions().values()
		);

		ViewRegistrations viewRegistrations = DatasetCreationUtils.createViewRegistrations(
				sequenceDescription.getViewDescriptions(),
				minResolution
		);

		ViewInterestPoints viewInterestPoints = new ViewInterestPoints();

		return new SpimData2(
				imagePath,
				sequenceDescription,
				viewRegistrations,
				viewInterestPoints,
				new BoundingBoxes(),
				new PointSpreadFunctions(),
				new StitchingResults(),
				new IntensityAdjustments()
		);
	}

	private void transformTileLocations(SequenceDescription sequenceDescription) {
		if (!swapXY && !flipX && !flipY) {
			return;
		}
		// view setups of the same tile may share one Tile object, transform every object once
		Set<Tile> transformed = Collections.newSetFromMap(new IdentityHashMap<>());
		for (ViewSetup vs : sequenceDescription.getViewSetupsOrdered()) {
			Tile tile = vs.getTile();
			double[] location = tile == null ? null : tile.getLocation();
			if (location == null || location.length < 2 || !transformed.add(tile)) {
				continue;
			}
			double[] newLocation = location.clone();
			if (swapXY) {
				newLocation[0] = location[1];
				newLocation[1] = location[0];
			}
			if (flipX) newLocation[0] = -newLocation[0];
			if (flipY) newLocation[1] = -newLocation[1];
			tile.setLocation(newLocation);
		}
		long nTiles = transformed.stream().mapToInt(Tile::getId).distinct().count();
		System.out.println("Transformed the stage positions of " + nTiles + " tiles (swap xy: " + swapXY + ", flip x: " + flipX + ", flip y: " + flipY + ")");
	}

	/**
	 * So far only local paths are supported.
	 *
	 * @param imageURI
	 * @return
	 */
	private List<StackFile> getStackFiles(URI imageURI)
	{
		int searchDepth = fileNamePattern.getSearchDepth();
		try {
			Path imagePath = Paths.get(imageURI);
			// get the files
			PathMatcher matcher = FileSystems.getDefault().getPathMatcher(fileNamePattern.getGlobPattern());
			List<StackFile> fs = Files.walk( imagePath , searchDepth+1)
					.filter(path -> matcher.matches(imagePath.relativize(path)))
					.map(p -> getStackFile(imageURI, imagePath.relativize(p).toString()))
					.collect(Collectors.toList());
			System.out.println(fs);
			return fs;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private StackFile getStackFile(URI imageURI, String imageRelativePath)
	{
		Matcher m = fileNamePattern.regexPattern.matcher(imageRelativePath);
		if ( m.matches() ) {
			int tp = extractInt(fileNamePattern.hasKey("t") ? m.group("tp") : "0"); // timepoint
			int ch = extractInt(fileNamePattern.hasKey("c") ? m.group("ch") : "0"); // channel
			int il = extractInt(fileNamePattern.hasKey("i") ? m.group("il") : "0"); // illumination
			int ang = extractInt(fileNamePattern.hasKey("a") ? m.group("ang") : "0"); // angle
			int ti = extractInt(fileNamePattern.hasKey("x") ? m.group("ti") : "0"); // tile
			return new StackFile(tp, ch, il, ang, ti, imageURI, imageRelativePath);
		} else {
			throw new IllegalArgumentException(imageRelativePath + " does not match " + fileNamePattern.sourcePattern + ". Refine the pattern and try again");
		}
	}

	int extractInt(String input) {
		Matcher m = Pattern.compile("\\D*(\\d+)").matcher(input);
		if (m.matches()) {
			return Integer.parseInt(m.group(1));
		} else {
			return 0;
		}
	}

	private static boolean allStackFilesHaveExtension(List<StackFile> stackFiles, String extension) {
		return !stackFiles.isEmpty() && stackFiles.stream()
				.allMatch(sf -> sf.relativeFilePath.toLowerCase().endsWith(extension));
	}

	private ViewSetupBuilder createViewSetupBuilder(URI imageURI, List<StackFile> stackFiles, TimePoints timePoints) {
		if ( imageURI.getPath().contains(".czi") || allStackFilesHaveExtension(stackFiles, ".czi") ) {
			return new CZIViewSetupBuilder(timePoints);
		} else if ( allStackFilesHaveExtension(stackFiles, ".nd2") ) {
			return new ND2ViewSetupBuilder(timePoints);
		} else if ( imageURI.getScheme().equals("n5") || imageURI.getScheme().equals("file") && imageURI.getPath().contains(".n5") ) {
			return new N5MultichannelViewSetupBuilder(imageURI, timePoints);
		} else {
			return new LOCIViewSetupBuilder(timePoints);
		}
	}
}

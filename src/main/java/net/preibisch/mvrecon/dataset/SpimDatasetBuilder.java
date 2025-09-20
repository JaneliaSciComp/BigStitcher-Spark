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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import loci.formats.ChannelSeparator;
import loci.formats.IFormatReader;
import loci.formats.meta.MetadataRetrieve;
import mpicbg.spim.data.generic.sequence.BasicViewDescription;
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
import net.preibisch.mvrecon.fiji.datasetmanager.DatasetCreationUtils;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBoxes;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.FileMapImgLoaderLOCI;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.LegacyStackImgLoaderLOCI;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.filemap2.FileMapEntry;
import net.preibisch.mvrecon.fiji.spimdata.intensityadjust.IntensityAdjustments;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.pointspreadfunctions.PointSpreadFunctions;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.StitchingResults;
import ome.units.UNITS;
import ome.units.quantity.Length;
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


	static class LOCIViewSetupBuilder implements ViewSetupBuilder {

		private final SequenceDescription sequenceDescription;
		private final Map<Integer, StackFile> viewToStackFileMap = new HashMap<>();

		LOCIViewSetupBuilder(TimePoints timePoints) {
			this.sequenceDescription = new SequenceDescription(
					timePoints,
					/*view setups*/Collections.emptyList()
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
						vs.getChannel().getId());
				fileMap.put( vdI,  fileMapEntry);
			}

			sequenceDescription.setImgLoader(new FileMapImgLoaderLOCI(
					fileMap,
					sequenceDescription,
					false
			));
			return this;
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
				IFormatReader formatReader = new ChannelSeparator();
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

					stackFile.nImages = retrieve.getImageCount();
					stackFile.nTp = formatReader.getSizeT();
					stackFile.nCh = formatReader.getSizeC();
					stackFile.sizeZ = formatReader.getSizeZ();
					stackFile.sizeY = formatReader.getSizeY();
					stackFile.sizeX = formatReader.getSizeX();
					for (int imageIndex = 0; imageIndex < stackFile.nImages; imageIndex++) {
						Length offsetX = retrieve.getPlanePositionX(imageIndex, 0);
						Length offsetY = retrieve.getPlanePositionY(imageIndex, 0);
						Length offsetZ = retrieve.getPlanePositionZ(imageIndex, 0);
						Length resX = retrieve.getPixelsPhysicalSizeX(imageIndex);
						Length resY = retrieve.getPixelsPhysicalSizeY(imageIndex);
						Length resZ = retrieve.getPixelsPhysicalSizeZ(imageIndex);

						double oX = offsetX != null ? offsetX.value(UNITS.MICROMETER).doubleValue() : 0;
						double oY = offsetY != null ? offsetY.value(UNITS.MICROMETER).doubleValue() : 0;
						double oZ = offsetZ != null ? offsetZ.value(UNITS.MICROMETER).doubleValue() : 0;
						double rX = resX != null ? resX.value(UNITS.MICROMETER).doubleValue() : 0;
						double rY = resY != null ? resY.value(UNITS.MICROMETER).doubleValue() : 0;
						double rZ = resZ != null ? resZ.value(UNITS.MICROMETER).doubleValue() : 0;

						int imageChannels = retrieve.getChannelCount(imageIndex);
						for (int chIndex = 0; chIndex < imageChannels; chIndex++) {
							String chName = retrieve.getChannelName(imageIndex, chIndex);
							// currently viewIndex is only based on the number of images and channels
							// but a correct implementation would also consider timepoints, illuminations and angles
							// for now I am ignoring them because so far we never needed them.
							int viewIndex = chIndex * nfiles * stackFile.nImages + sfi * stackFile.nImages + imageIndex;
							Tile tile = new Tile(stackFile.getTi() * stackFile.nImages + imageIndex);
							tile.setLocation(new double[]{oX, oY, oZ});
							Channel channel = new Channel(chIndex, chName);
							ViewSetup vs = new ViewSetup(
									viewIndex,
									String.valueOf(viewIndex),
									new FinalDimensions(stackFile.sizeX, stackFile.sizeY, stackFile.sizeZ),
									new FinalVoxelDimensions("um", rX, rY, rZ),
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
	}

	static class N5ViewSetupBuilder implements ViewSetupBuilder {

		private final URI n5ContainerURI;
		private final SequenceDescription sequenceDescription;
		private final Map<ViewId, String> viewIdToPath;
		private final N5Reader n5Reader;
		private final N5MultichannelProperties n5MultichannelProperties;

		public N5ViewSetupBuilder(URI n5ContainerURI, TimePoints timePoints) {
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
		public N5ViewSetupBuilder setImgLoader() {
			sequenceDescription.setImgLoader(
					new N5MultichannelLoader( n5ContainerURI, StorageFormat.N5, sequenceDescription, viewIdToPath )
			);
			return this;
		}

		@SuppressWarnings("unchecked")
		@Override
		public N5ViewSetupBuilder createViewSetups(List<StackFile> stackFiles) {
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
		final String globPattern;
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
					.replaceAll("\\{t\\}", "(?<tp>\\\\D*?\\\\d+)")
					.replaceAll("\\{c\\}", "(?<ch>\\\\D*?\\\\d+)")
					.replaceAll("\\{i\\}", "(?<il>\\\\D*?\\\\d+)")
					.replaceAll("\\{a\\}", "(?<ang>\\\\D*?\\\\d+)")
					.replaceAll("\\{x\\}", "(?<ti>\\\\D*?\\\\d+)") );

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

	public SpimDatasetBuilder(String fileNamePattern )
	{
		this.fileNamePattern = new StackPattern(fileNamePattern);
	}

	public SpimData2 createDataset(URI imagePath) {
		List<StackFile> stackFiles = getStackFiles(imagePath);

		// collect timepoints from stack files
		Set<TimePoint> timePoints = stackFiles.stream()
				.map(si -> new TimePoint(si.viewIndex.tp))
				.collect(Collectors.toSet());

		SequenceDescription sequenceDescription = createViewSetupBuilder(imagePath, new TimePoints(timePoints))
				.createViewSetups(stackFiles)
				.setImgLoader()
				.getSequenceDescription();

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
			int tp = extractInt(fileNamePattern.hasKey("t") ? m.group("tp") : "0");
			int ch = extractInt(fileNamePattern.hasKey("c") ? m.group("ch") : "0");
			int il = extractInt(fileNamePattern.hasKey("i") ? m.group("il") : "0");
			int ang = extractInt(fileNamePattern.hasKey("a") ? m.group("ang") : "0");
			int ti = extractInt(fileNamePattern.hasKey("x") ? m.group("ti") : "0");
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

	private ViewSetupBuilder createViewSetupBuilder(URI imageURI, TimePoints timePoints) {
		if ( imageURI.getScheme().equals("n5") || imageURI.getScheme().equals("file") && imageURI.getPath().contains(".n5") ) {
			return new N5ViewSetupBuilder(imageURI, timePoints);
		} else {
			return new LOCIViewSetupBuilder(timePoints);
		}
	}
}

package net.preibisch.mvrecon.dataset;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
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
import mpicbg.spim.data.sequence.ImgLoader;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.Tile;
import mpicbg.spim.data.sequence.TimePoint;
import mpicbg.spim.data.sequence.TimePoints;
import mpicbg.spim.data.sequence.ViewDescription;
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
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.filemap2.FileMapImgLoaderLOCI2;
import net.preibisch.mvrecon.fiji.spimdata.intensityadjust.IntensityAdjustments;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.pointspreadfunctions.PointSpreadFunctions;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.StitchingResults;
import ome.units.UNITS;
import ome.units.quantity.Length;
import org.apache.commons.lang3.builder.ToStringBuilder;
import scala.Tuple2;

public class DatasetBuilder {

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

	static class TileInfo {
		final int tileIndex; // global tile (view) index
		final Path filePath; // file containing this tile
		final int tp;
		final int tileName;
		final int imageIndex;
		final int chIndex;
		final String chName;
		final int angle;
		final int illumination;
		final int sizeZ;
		final int sizeY;
		final int sizeX;
		final double z;
		final double y;
		final double x;
		final double resZ;
		final double resY;
		final double resX;

		TileInfo(int tileIndex,
				 Path filePath,
				 int tp,
				 int tileName,
				 int imageIndex,
				 int chIndex, String chName,
				 int angle, int illumination,
				 int sizeZ, int sizeY, int sizeX,
				 double z, double y, double x,
				 double resZ, double resY, double resX) {
			this.tileIndex = tileIndex;
			this.filePath = filePath;
			this.tp = tp;
			this.tileName = tileName;
			this.imageIndex = imageIndex;
			this.chIndex = chIndex;
			this.chName = chName;
			this.angle = angle;
			this.illumination = illumination;
			this.sizeZ = sizeZ;
			this.sizeY = sizeY;
			this.sizeX = sizeX;
			this.z = z;
			this.y = y;
			this.x = x;
			this.resZ = resZ;
			this.resY = resY;
			this.resX = resX;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null || getClass() != o.getClass()) return false;
			TileInfo tileInfo = (TileInfo) o;
			return tp == tileInfo.tp && imageIndex == tileInfo.imageIndex && tileIndex == tileInfo.tileIndex && chIndex == tileInfo.chIndex && angle == tileInfo.angle && illumination == tileInfo.illumination && Objects.equals(filePath, tileInfo.filePath) && Objects.equals(chName, tileInfo.chName);
		}

		@Override
		public int hashCode() {
			return Objects.hash(filePath, tp, imageIndex, tileIndex, chIndex, chName, angle, illumination);
		}
	}

	static class StackFile {
		final ViewIndex viewIndex;
		final int ti;
		final Path file;
		int nImages = -1;
		int nTp = -1;
		int nCh = -1;
		int sizeZ = -1;
		int sizeY = -1;
		int sizeX = -1;

		StackFile(int tp, int ch, int il, int ang, int ti, Path file )
		{
			this.viewIndex = new ViewIndex(tp, ch, il, ang);
			this.ti = ti;
			this.file = file;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null || getClass() != o.getClass()) return false;
			StackFile stackFile = (StackFile) o;
			return ti == stackFile.ti && Objects.equals(viewIndex, stackFile.viewIndex) && Objects.equals(file, stackFile.file);
		}

		@Override
		public int hashCode() {
			return Objects.hash(viewIndex, ti, file);
		}

		@Override
		public String toString() {
			return new ToStringBuilder(this)
					.append("view", viewIndex)
					.append("ti", ti)
					.append("file", file)
					.toString();
		}

		List<TileInfo> loadTileMetadata()
		{
			List<TileInfo> tiles = new ArrayList<>();
			if ( !file.toFile().exists() )
			{
				return tiles;
			}

			IFormatReader formatReader = new ChannelSeparator();
			try {
				if ( !LegacyStackImgLoaderLOCI.createOMEXMLMetadata( formatReader ) ) {
					try {
						formatReader.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					return tiles;
				}

				formatReader.setId( file.toString() );

				MetadataRetrieve retrieve = (MetadataRetrieve)formatReader.getMetadataStore();

				this.nImages = retrieve.getImageCount();
				this.nTp = formatReader.getSizeT();
				this.nCh = formatReader.getSizeC();
				this.sizeZ = formatReader.getSizeZ();
				this.sizeY = formatReader.getSizeY();
				this.sizeX = formatReader.getSizeX();
				for (int imageIndex = 0; imageIndex < nImages; imageIndex++) {
					Length z = retrieve.getPlanePositionZ(imageIndex, 0);
					Length y = retrieve.getPlanePositionY(imageIndex, 0);
					Length x = retrieve.getPlanePositionX(imageIndex, 0);
					Length resX = retrieve.getPixelsPhysicalSizeX(imageIndex);
					Length resY = retrieve.getPixelsPhysicalSizeY(imageIndex);
					Length resZ = retrieve.getPixelsPhysicalSizeZ(imageIndex);
					double zz = z != null ? z.value(UNITS.MICROMETER).doubleValue() : 0;
					double yy = y != null ? y.value(UNITS.MICROMETER).doubleValue() : 0;
					double xx = x != null ? x.value(UNITS.MICROMETER).doubleValue() : 0;
					double rZ = resZ != null ? resZ.value(UNITS.MICROMETER).doubleValue() : 0;
					double rY = resY != null ? resY.value(UNITS.MICROMETER).doubleValue() : 0;
					double rX = resX != null ? resX.value(UNITS.MICROMETER).doubleValue() : 0;
					int imageChannels = retrieve.getChannelCount(imageIndex);
					for (int chIndex = 0; chIndex < imageChannels; chIndex++) {
						String chName = retrieve.getChannelName(imageIndex, chIndex);
						tiles.add(new TileInfo(
								(ti * nImages + imageIndex) * imageChannels + chIndex,
								file,
								viewIndex.tp,
								ti,
								imageIndex,
								chIndex, chName,
								viewIndex.ang, viewIndex.il,
								sizeZ, sizeY, sizeX,
								zz, yy, xx,
								rZ, rY, rX));
					}
				}
				return tiles;
			} catch (Exception e) {
				throw new IllegalStateException("Could not read " + file, e);
			}
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

	public DatasetBuilder(String fileNamePattern )
	{
		this.fileNamePattern = new StackPattern(fileNamePattern);
	}

	public SpimData2 createDataset(String imageDir) {
		Path imagePath = Paths.get(imageDir);
		List<StackFile> stackFiles = getStackFiles(imagePath);

		// collect timepoints from stack files
		Set<TimePoint> timePoints = stackFiles.stream()
				.map(si -> new TimePoint(si.viewIndex.tp))
				.collect(Collectors.toSet());

		// create view setups
		Map<TileInfo, ViewSetup> viewSetups = stackFiles.stream()
				.flatMap(sf -> sf.loadTileMetadata().stream())
				.map(tileInfo -> {
					Dimensions size = new FinalDimensions(tileInfo.sizeX, tileInfo.sizeY, tileInfo.sizeZ);
					VoxelDimensions voxelSize = new FinalVoxelDimensions("um", tileInfo.resX, tileInfo.resY, tileInfo.resZ);
					return new Tuple2<>(
							tileInfo,
							new ViewSetup(
									tileInfo.tileIndex,
									tileInfo.chName,
									size,
									voxelSize,
									new Tile(tileInfo.imageIndex),
									new Channel(tileInfo.chIndex),
									new Angle(tileInfo.angle),
									new Illumination(tileInfo.illumination)
							)
					);
				})
				.collect(Collectors.toMap(t -> t._1, t -> t._2));

		SequenceDescription sequenceDescription = new SequenceDescription(
				new TimePoints(timePoints),
				viewSetups.values(),
				/*image loader*/null,
				null // missing views not handled for now
		);

		ImgLoader imgLoader = createImageLoader(imagePath, viewSetups.keySet(), sequenceDescription);

		sequenceDescription.setImgLoader(imgLoader);

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
				imagePath.toUri(),
				sequenceDescription,
				viewRegistrations,
				viewInterestPoints,
				new BoundingBoxes(),
				new PointSpreadFunctions(),
				new StitchingResults(),
				new IntensityAdjustments()
		);
	}

	private ImgLoader createImageLoader(Path imagePath, Collection<TileInfo> tileInfos, SequenceDescription sd) {
		Map<BasicViewDescription< ? >, FileMapEntry> fileMap = new HashMap<>();
		for (TileInfo ti : tileInfos) {
			ViewDescription vdI = sd.getViewDescription( ti.tp, ti.tileIndex );
			fileMap.put( vdI, new FileMapEntry(ti.filePath.toFile(), ti.imageIndex, ti.chIndex) );
		}

		return new FileMapImgLoaderLOCI(
				fileMap,
				sd,
				false
		);
	}

	private List<StackFile> getStackFiles(Path imagePath)
	{
		int searchDepth = fileNamePattern.getSearchDepth();
		try {
			// get the files
			PathMatcher matcher = FileSystems.getDefault().getPathMatcher(fileNamePattern.getGlobPattern());
			List<StackFile> fs = Files.walk( imagePath , searchDepth+1)
					.filter(path -> matcher.matches(imagePath.relativize(path)))
					.map(p -> getStackFile(imagePath.relativize(p).toString(), p))
					.collect(Collectors.toList());
			System.out.println(fs);
			return fs;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private StackFile getStackFile(String matchingPattern, Path filePath)
	{
		Matcher m = fileNamePattern.regexPattern.matcher(matchingPattern);
		if ( m.matches() ) {
			int tp = extractInt(fileNamePattern.hasKey("t") ? m.group("tp") : "0");
			int ch = extractInt(fileNamePattern.hasKey("c") ? m.group("ch") : "0");
			int il = extractInt(fileNamePattern.hasKey("i") ? m.group("il") : "0");
			int ang = extractInt(fileNamePattern.hasKey("a") ? m.group("ang") : "0");
			int ti = extractInt(fileNamePattern.hasKey("x") ? m.group("ti") : "0");
			return new StackFile(tp, ch, il, ang, ti, filePath);
		} else {
			throw new IllegalArgumentException(matchingPattern + " does not match " + fileNamePattern.sourcePattern + ". Refine the pattern and try again");
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
}

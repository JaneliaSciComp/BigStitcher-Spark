package net.preibisch.mvrecon.fiji.spimdata.imgloaders.filemap2;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import com.google.common.io.Files;

import ch.epfl.biop.formats.in.ZeissQuickStartCZIReader;
import loci.formats.IFormatReader;
import loci.formats.Memoizer;
import loci.formats.in.DynamicMetadataOptions;
import loci.formats.in.MetadataOptions;
import mpicbg.spim.data.generic.sequence.AbstractSequenceDescription;
import mpicbg.spim.data.generic.sequence.BasicViewDescription;
import mpicbg.spim.data.generic.sequence.BasicViewSetup;
import mpicbg.spim.data.generic.sequence.ImgLoaderHint;
import mpicbg.spim.data.sequence.ImgLoader;
import mpicbg.spim.data.sequence.SetupImgLoader;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.Dimensions;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.Cache;
import net.imglib2.cache.ref.WeakRefLoaderCache;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.CloseableThreadLocal;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.util.BioformatsReaderSetupHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ImgLib2Tools;

/**
 * {@link ImgLoader} for CZI files that reads pixel data through
 * {@link ZeissQuickStartCZIReader} instead of the LOCI {@code ZeissCZIReader}.
 * <p>
 * Mirrors {@link FileMapImgLoaderLOCI2}, but each {@link IFormatReader}
 * produced by the internal supplier is a {@link ZeissQuickStartCZIReader}
 * wrapped in a {@link Memoizer} and pre-configured by a
 */
public class CZIImageLoader implements ImgLoader, FileMapGettable {
    private static final Logger LOG = LoggerFactory.getLogger(CZIImageLoader.class);

    private final Map<ViewId, FileMapEntry> fileMap;

    private final File tempDir;

    private final AbstractSequenceDescription<?, ?, ?> sd;

    private final boolean allTimepointsInSingleFiles;

    private final Map<Integer, SetupImgLoader<?>> setupImgLoaders = new ConcurrentHashMap<>();

    private final Supplier<IFormatReader> getReader;

    public CZIImageLoader(
            final Map<? extends ViewId, FileMapEntry> fileMap,
            final AbstractSequenceDescription<?, ?, ?> sequenceDescription) {
        this(fileMap, sequenceDescription, false, true);
    }

    public CZIImageLoader(
            final Map<? extends ViewId, FileMapEntry> fileMap,
            final AbstractSequenceDescription<?, ?, ?> sequenceDescription,
            final boolean allowAutostitch,
            final boolean relativePositions) {
        this.fileMap = new HashMap<>(fileMap);
        this.tempDir = Files.createTempDir();
        this.sd = sequenceDescription;

        // populate map file -> {time points} to detect single-timepoint files
        final Map<File, Set<Integer>> tpsPerFile = new HashMap<>();
        boolean allTimepointsInSingleFiles = true;
        for (final Map.Entry<? extends ViewId, FileMapEntry> entry : fileMap.entrySet()) {
            final ViewId vid = entry.getKey();
            final File file = entry.getValue().file();
            final Set<Integer> tps = tpsPerFile.computeIfAbsent(file, k -> new HashSet<>());
            tps.add(vid.getTimePointId());

            if (tps.size() > 1) {
                allTimepointsInSingleFiles = false;
                break;
            }
        }
        this.allTimepointsInSingleFiles = allTimepointsInSingleFiles;

        this.getReader = () -> {
            LOG.info("Create ZeissQuickStartCZIReader");
            final ZeissQuickStartCZIReader cziReader = new ZeissQuickStartCZIReader();
            configureCZIReader(cziReader, allowAutostitch, relativePositions);
            return new Memoizer(cziReader, Memoizer.DEFAULT_MINIMUM_ELAPSED, tempDir);
        };
    }

    private void configureCZIReader(IFormatReader reader, boolean allowAutostitch, boolean relativePositions) {
        // disable auto stitching, following solutions by @CellKai and @NicoKiaru in
        // https://forum.image.sc/t/change-in-czi-tile-info-metadata-after-upgrade-to-zeiss-lightsheet-7/49414/15
        final MetadataOptions options = reader.getMetadataOptions();
        if (options instanceof DynamicMetadataOptions dynamicOptions) {
            dynamicOptions.setBoolean(ZeissQuickStartCZIReader.ALLOW_AUTOSTITCHING_KEY, allowAutostitch);
            dynamicOptions.setBoolean(ZeissQuickStartCZIReader.RELATIVE_POSITIONS_KEY, relativePositions);
            reader.setMetadataOptions(dynamicOptions);
        } else {
            System.err.println("WARNING: could not set CZI metadata options for " + reader.getClass().getSimpleName() + ".");
        }
    }

    @Override
    public SetupImgLoader<?> getSetupImgLoader(int setupId) {
        return setupImgLoaders.computeIfAbsent(setupId, CZISetupImgLoader::new);
    }

    @Override
    public Map<ViewId, FileMapEntry> getFileMap() {
        return fileMap;
    }

    public class CZISetupImgLoader<T extends RealType<T> & NativeType<T>> implements SetupImgLoader<T> {
        private final int setupId;

        private final CloseableThreadLocal<IFormatReader> threadLocalReader = CloseableThreadLocal.withInitial(getReader);

        private final Cache<Integer, RandomAccessibleInterval<T>> images = new WeakRefLoaderCache<Integer, RandomAccessibleInterval<T>>().withLoader(this::createImage);

        private final Supplier<T> type;

        public CZISetupImgLoader(int setupId) {
            this.setupId = setupId;
            this.type = lazyInit(() -> {
                final BasicViewDescription<?> aVd = getAnyPresentViewDescriptionForViewSetup(sd, setupId);
                if (aVd == null)
                    return null;
                final FileMapEntry entry = fileMap.get(aVd);
                return VirtualRAIFactoryLOCI.getType(threadLocalReader::get, entry.file(), entry.series());
            });
        }

        private RandomAccessibleInterval<T> createImage(final int timepointId) {
            final FileMapEntry entry = fileMap.get(new ViewId(timepointId, setupId));
            return VirtualRAIFactoryLOCI.createVirtualCached(
                    threadLocalReader::get,
                    entry.file(),
                    entry.series(),
                    entry.channel(),
                    allTimepointsInSingleFiles ? 0 : timepointId);
        }

        @Override
        public RandomAccessibleInterval<T> getImage(final int timepointId, final ImgLoaderHint... hints) {
            try {
                return images.get(timepointId);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public T getImageType() {
            return type.get();
        }

        @Override
        public RandomAccessibleInterval<FloatType> getFloatImage(int timepointId, boolean normalize,
                                                                 ImgLoaderHint... hints) {
            if (normalize)
                return ImgLib2Tools.normalizeVirtualRAI(getImage(timepointId, hints));
            else
                return ImgLib2Tools.convertVirtualRAI(getImage(timepointId, hints));
        }

        @Override
        public Dimensions getImageSize(int timepointId) {
            return getViewSetup(timepointId).getSize();
        }

        @Override
        public VoxelDimensions getVoxelSize(int timepointId) {
            return getViewSetup(timepointId).getVoxelSize();
        }

        private BasicViewSetup getViewSetup(int timepointId) {
            BasicViewDescription<?> vd = sd.getViewDescriptions().get(new ViewId(timepointId, setupId));
            return vd.getViewSetup();
        }
    }

    private static BasicViewDescription<?> getAnyPresentViewDescriptionForViewSetup(
            AbstractSequenceDescription<?, ?, ?> sd, int viewSetupId) {
        for (final ViewId vid : sd.getViewDescriptions().keySet())
            if (vid.getViewSetupId() == viewSetupId)
                if (!sd.getMissingViews().getMissingViews().contains(vid))
                    return sd.getViewDescriptions().get(vid);

        return null;
    }

    private static <T> Supplier<T> lazyInit(final Supplier<T> supplier) {
        return new Supplier<T>() {
            T value = null;

            @Override
            public synchronized T get() {
                if (value == null)
                    value = supplier.get();
                return value;
            }
        };
    }
}

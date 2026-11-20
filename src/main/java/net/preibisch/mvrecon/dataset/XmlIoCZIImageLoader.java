package net.preibisch.mvrecon.dataset;

import static mpicbg.spim.data.XmlKeys.IMGLOADER_FORMAT_ATTRIBUTE_NAME;

import java.io.File;
import java.net.URI;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.jdom2.Element;

import mpicbg.spim.data.XmlHelpers;
import mpicbg.spim.data.generic.sequence.AbstractSequenceDescription;
import mpicbg.spim.data.generic.sequence.ImgLoaderIo;
import mpicbg.spim.data.generic.sequence.XmlIoBasicImgLoader;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.filemap2.CZIImageLoader;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.filemap2.FileMapEntry;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.filemap2.FileMapImgLoaderLOCI2;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.filemap2.XmlIOFileMapImgLoaderLOCI2;
import util.URITools;

/**
 * Saves a {@link CZIImageLoader} as a {@code spimreconstruction.filemap2} image loader (the XML of
 * {@link XmlIOFileMapImgLoaderLOCI2}), so the project opens in every tool that knows mvr, including Fiji.
 * Such an XML is loaded back as a {@link FileMapImgLoaderLOCI2}.
 *
 * The format of the annotation is a name of its own on purpose: spim_data maps every format to one
 * XmlIo class when loading, and a second class claiming "spimreconstruction.filemap2" would take over
 * the loading of every filemap2 project. The annotation only registers this class for saving a
 * CZIImageLoader; {@link #fromXml} reads the same element into a CZIImageLoader when called directly
 * (or for an ImageLoader element whose format is {@link #FORMAT}).
 */
@ImgLoaderIo( format = XmlIoCZIImageLoader.FORMAT, type = CZIImageLoader.class )
public class XmlIoCZIImageLoader implements XmlIoBasicImgLoader< CZIImageLoader >
{
	static final String FORMAT = "spimreconstruction.filemap2";

	@Override
	public Element toXml( final CZIImageLoader imgLoader, final URI basePathURI )
	{
		return toXml( imgLoader, new File( URITools.fromURI( basePathURI ) ) );
	}

	@Override
	public Element toXml( final CZIImageLoader imgLoader, final File basePath )
	{
		final Element wholeElem = new Element( "ImageLoader" );
		wholeElem.setAttribute( IMGLOADER_FORMAT_ATTRIBUTE_NAME, FORMAT );
		wholeElem.addContent( XmlHelpers.booleanElement( XmlIOFileMapImgLoaderLOCI2.ZGROUPED_TAG, false ) );

		final Element filesElement = new Element( XmlIOFileMapImgLoaderLOCI2.FILES_TAG );
		imgLoader.getFileMap().entrySet().stream()
				.sorted( Map.Entry.comparingByKey( Comparator
						.comparingInt( ViewId::getTimePointId )
						.thenComparingInt( ViewId::getViewSetupId ) ) )
				.forEach( e -> {
					final ViewId vid = e.getKey();
					final FileMapEntry entry = e.getValue();
					final Element fileMappingElement = new Element( XmlIOFileMapImgLoaderLOCI2.FILE_MAPPING_TAG );
					fileMappingElement.setAttribute( XmlIOFileMapImgLoaderLOCI2.MAPPING_VS_TAG, Integer.toString( vid.getViewSetupId() ) );
					fileMappingElement.setAttribute( XmlIOFileMapImgLoaderLOCI2.MAPPING_TP_TAG, Integer.toString( vid.getTimePointId() ) );
					fileMappingElement.addContent( XmlHelpers.pathElement( XmlIOFileMapImgLoaderLOCI2.MAPPING_FILE_TAG, entry.file(), basePath ) );
					fileMappingElement.setAttribute( XmlIOFileMapImgLoaderLOCI2.MAPPING_SERIES_TAG, Integer.toString( entry.series() ) );
					fileMappingElement.setAttribute( XmlIOFileMapImgLoaderLOCI2.MAPPING_C_TAG, Integer.toString( entry.channel() ) );
					filesElement.addContent( fileMappingElement );
				} );
		wholeElem.addContent( filesElement );

		return wholeElem;
	}

	@Override
	public CZIImageLoader fromXml(
			final Element elem,
			final URI basePathURI,
			final AbstractSequenceDescription< ?, ?, ? > sequenceDescription )
	{
		return fromXml( elem, new File( URITools.fromURI( basePathURI ) ), sequenceDescription );
	}

	@Override
	public CZIImageLoader fromXml(
			final Element elem,
			final File basePath,
			final AbstractSequenceDescription< ?, ?, ? > sequenceDescription )
	{
		final Element filesElement = elem.getChild( XmlIOFileMapImgLoaderLOCI2.FILES_TAG );
		if ( filesElement == null )
			throw new IllegalArgumentException( "ImageLoader element has no <" + XmlIOFileMapImgLoaderLOCI2.FILES_TAG + ">" );

		final Map< ViewId, FileMapEntry > fileMap = new HashMap<>();
		for ( final Element e : filesElement.getChildren( XmlIOFileMapImgLoaderLOCI2.FILE_MAPPING_TAG ) )
		{
			final int vs = Integer.parseInt( e.getAttributeValue( XmlIOFileMapImgLoaderLOCI2.MAPPING_VS_TAG ) );
			final int tp = Integer.parseInt( e.getAttributeValue( XmlIOFileMapImgLoaderLOCI2.MAPPING_TP_TAG ) );
			final int series = Integer.parseInt( e.getAttributeValue( XmlIOFileMapImgLoaderLOCI2.MAPPING_SERIES_TAG ) );
			final int channel = Integer.parseInt( e.getAttributeValue( XmlIOFileMapImgLoaderLOCI2.MAPPING_C_TAG ) );
			final File file = XmlHelpers.loadPath( e, XmlIOFileMapImgLoaderLOCI2.MAPPING_FILE_TAG, basePath );
			fileMap.put( new ViewId( tp, vs ), new FileMapEntry( file, series, channel ) );
		}

		return new CZIImageLoader( fileMap, sequenceDescription );
	}
}

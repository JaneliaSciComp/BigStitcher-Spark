package net.preibisch.mvrecon.dataset;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import mpicbg.spim.data.XmlHelpers;
import mpicbg.spim.data.generic.sequence.AbstractSequenceDescription;
import mpicbg.spim.data.generic.sequence.ImgLoaderIo;
import mpicbg.spim.data.generic.sequence.XmlIoBasicImgLoader;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.AllenOMEZarrLoader;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.jdom2.Element;

import static mpicbg.spim.data.XmlKeys.IMGLOADER_FORMAT_ATTRIBUTE_NAME;

@ImgLoaderIo( format = "bdv.multchimg.n5", type = N5MultichannelLoader.class )
public class XmlToN5MultichannelLoader implements XmlIoBasicImgLoader<N5MultichannelLoader> {
	@Override
	public Element toXml(N5MultichannelLoader imgLoader, File basePath) {
		final Element imgLoaderElement = new Element( "ImageLoader" );
		imgLoaderElement.setAttribute( IMGLOADER_FORMAT_ATTRIBUTE_NAME, "bdv.multchimg.n5" );
		imgLoaderElement.setAttribute( "version", "1.0" );

		imgLoaderElement.addContent( XmlHelpers.pathElementURI( "n5", imgLoader.getN5URI(), basePath.toURI() ));

		final Element zgroupsElement = new Element( "n5groups" );

		for ( final Map.Entry<ViewId, String> entry : imgLoader.getViewIdToPath().entrySet() )
		{
			final Element n5groupElement = new Element("n5group");
			n5groupElement.setAttribute( "setup", String.valueOf( entry.getKey().getViewSetupId() ) );
			n5groupElement.setAttribute( "tp", String.valueOf( entry.getKey().getTimePointId() ) );
			n5groupElement.setAttribute( "path", String.valueOf( entry.getValue() ) );

			zgroupsElement.addContent( n5groupElement );
		}

		imgLoaderElement.addContent( zgroupsElement );

		return imgLoaderElement;
	}

	@Override
	public N5MultichannelLoader fromXml(Element elem, File basePath, AbstractSequenceDescription<?, ?, ?> sequenceDescription) {
		final Map<ViewId, String> n5groups = new HashMap<>();

		URI uri = XmlHelpers.loadPathURI( elem, "n5", basePath.toURI() );

		final Element n5groupsElem = elem.getChild( "n5groups" );
		for ( final Element c : n5groupsElem.getChildren( "n5group" ) )
		{
			final int timepointId = Integer.parseInt( c.getAttributeValue( "tp" ) );
			final int setupId = Integer.parseInt( c.getAttributeValue( "setup" ) );
			final String path = c.getAttributeValue( "path" );
			n5groups.put( new ViewId( timepointId, setupId ), path );
		}

		return new N5MultichannelLoader(uri, StorageFormat.N5, sequenceDescription, n5groups);
	}
}

/*-
 * #%L
 * Spark-based parallel BigStitcher project.
 * %%
 * Copyright (C) 2021 - 2024 Developers.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */
package net.preibisch.bigstitcher.spark.lens;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import net.preibisch.bigstitcher.spark.lens.LensModels.Model;

/**
 * Unit tests for the lens-model JSON parsing and channel matching
 * ({@link LensModels}). Uses two real entries (Cam1-T1 / Cam2-T1, fitted at
 * 1920x1920) so the mpicbg {@code NonLinearCoordinateTransform.init} succeeds.
 */
public class TestLensModels
{
	// first two entries of the user's lens JSON (Cam1-T1 498nm, Cam2-T1 648nm), 1920x1920
	private static final String JSON =
			"[\n" +
			"  {\n" +
			"    \"name\": \"lightsheet, Cam1-T1, G4_SG1, 498nm em\",\n" +
			"    \"transform\": [\n" +
			"      { \"className\": \"mpicbg.trakem2.transform.NonLinearCoordinateTransform\",\n" +
			"        \"dataString\": \"5 21 523.9256859065475 -5.788251886064326 -3.6312622123594416 447.6821430532968 25.942980287530588 8.333507852630476 10.695087213856262 13.262365381475547 0.3693793279076374 20.918516204988308 -33.907850323448216 -6.26286751701938 -7.0894801010486965 -15.390276405073784 -11.51042230116645 -9.259808044679268 12.714993029291733 -24.217905107473257 24.116736745939345 5.472871830538608 4.559306617831318 4.919167640236811 5.450296533524632 8.817238034233995 2.853067808531886 5.3078967745195165 -14.17324098609275 11.858267990653758 -8.948732376793364 -1.5072665850898623 0.6836825526139374 -2.6374126765159214 -3.916119022217482 0.20611479340811734 -0.2618773819640392 -4.262840463343759 -0.5881944497571392 -0.6476397625155093 5.247385285442615 -2.849826389436309 9.967163972908892 12.355632102253056 996.7162289175571 1235.5651486604156 1277950.6126742004 1218951.8909782043 1733870.0180303047 1.813729398940428E9 1.5524188496945558E9 1.6981049230263855E9 2.580381936677118E9 2.7240346123658906E12 2.1949689152785562E12 2.1491010972665256E12 2.512113549168532E12 3.990977200920535E12 4.2479178617584575E15 3.290054469274561E15 3.0241521978264655E15 3.1609908640136325E15 3.8653913205782755E15 6.351913187685029E15 100.0 533.4207681585224 455.27012380013025 1044506.1951552954 814449.5838638389 992359.3008431204 1.8760534889710898E9 1.4512497035954762E9 1.398063855311708E9 1.918464980751096E9 3.3381980894427363E12 2.5283327187867773E12 2.3177915229303687E12 2.449381942487628E12 3.5893775356174536E12 5.956731574309895E15 4.429917870533826E15 3.9144622443732535E15 3.8768538161809525E15 4.344383421804769E15 6.631167186012947E15 0.0 1920 1920 \" },\n" +
			"      { \"className\": \"mpicbg.trakem2.transform.AffineModel2D\",\n" +
			"        \"dataString\": \"0.9999999996993402 -2.4521822096482585E-5 2.4521822096482585E-5 0.9999999996993402 -59.138446453712106 0.3534456072865908\" }\n" +
			"    ]\n" +
			"  },\n" +
			"  {\n" +
			"    \"name\": \"lightsheet, Cam2-T1, G4_SG1, 648nm em\",\n" +
			"    \"transform\": [\n" +
			"      { \"className\": \"mpicbg.trakem2.transform.NonLinearCoordinateTransform\",\n" +
			"        \"dataString\": \"5 21 504.50159109772 -2.8082918508766603 -2.2731465350431996 388.45835228058496 3.7019971641169604 4.838675377231709 6.338459747607482 1.462101607943879 -2.1951085186786212 3.0148302625568384 7.773222420194024 -9.98979522361401 -1.2518508872380067 3.2565656757319488 -6.879960842924124 3.6546442561496235 15.514999831367895 6.58764351440351 -12.656520009097306 13.789923087883025 1.322035528798633 -5.164122595439601 3.7392105334786585 -7.52662111975971 -1.02280938876181 1.6741556096942087 -16.243723650079858 -13.174591859562327 4.122960279463404 -5.648410226248444 -0.8638923044599549 0.4634546820659331 -0.09047040462043654 2.941590848124747 -3.041212929737837 1.5102587079383785 2.556197538057414 -1.9879562671901443 5.492621391819558 5.198927059486039 9.857919391044632 12.652251219383063 985.7923971367816 1265.2264032595015 1231008.1932975447 1237316.1675937711 1754034.7732477363 1.714427323511734E9 1.5313580484956176E9 1.7139047212301533E9 2.5495723555436273E9 2.5345397203948022E12 2.1173420897822507E12 2.114473250982999E12 2.4938380488237197E12 3.8291015394652837E12 3.8931934849163725E15 3.1126281306851285E15 2.914242848093387E15 3.0717030309470205E15 3.7517068959193275E15 5.899532021054301E15 100.0 509.17840061632444 391.4859104332615 1009613.8569180776 763946.6421985846 867515.3369249769 1.79147670422991E9 1.388256077227951E9 1.2957839021720896E9 1.66830708489941E9 3.1424565664946523E12 2.397574289008106E12 2.1869732387328757E12 2.2364098114319067E12 3.0869280151233955E12 5.542847476836817E15 4.1443799868609085E15 3.6736568925438495E15 3.594978454609223E15 3.9044713685393305E15 5.63008939886453E15 0.0 1920 1920 \" },\n" +
			"      { \"className\": \"mpicbg.trakem2.transform.AffineModel2D\",\n" +
			"        \"dataString\": \"0.9999999996993402 -2.4521822096482585E-5 2.4521822096482585E-5 0.9999999996993402 55.658690398584966 -1.4718507289856881\" }\n" +
			"    ]\n" +
			"  }\n" +
			"]\n";

	@Test
	public void testLoadAndFittedSize() throws Exception
	{
		final List< Model > models = LensModels.load( JSON, "test" );
		assertEquals( 2, models.size() );

		final Model m0 = models.get( 0 );
		assertTrue( m0.name.contains( "Cam1-T1" ) );
		assertEquals( 1920, m0.fittedWidth );
		assertEquals( 1920, m0.fittedHeight );
		assertNotNull( m0.nonLinear );
		assertNotNull( m0.affine );

		// nonlinear + affine
		assertNotNull( m0.toTransform( true ) );
		// nonlinear only
		assertNotNull( m0.toTransform( false ) );
	}

	@Test
	public void testFindForChannelSubstring() throws Exception
	{
		final List< Model > models = LensModels.load( JSON, "test" );

		final Model cam1 = LensModels.findForChannel( models, "Cam1-T1" );
		assertNotNull( cam1 );
		assertTrue( cam1.name.contains( "Cam1-T1" ) );

		final Model cam2 = LensModels.findForChannel( models, "Cam2-T1" );
		assertNotNull( cam2 );
		assertTrue( cam2.name.contains( "Cam2-T1" ) );

		// no match -> null (view will be flatfield-only)
		assertNull( LensModels.findForChannel( models, "Cam9-T9" ) );
		// null / empty channel name -> null (no match)
		assertNull( LensModels.findForChannel( models, null ) );
		assertNull( LensModels.findForChannel( models, "" ) );
	}

	@Test
	public void testAmbiguousMatchThrows() throws Exception
	{
		final List< Model > models = LensModels.load( JSON, "test" );
		// "Cam" is a substring of both entry names -> ambiguous
		assertThrows( IllegalArgumentException.class, () -> LensModels.findForChannel( models, "Cam" ) );
	}
}

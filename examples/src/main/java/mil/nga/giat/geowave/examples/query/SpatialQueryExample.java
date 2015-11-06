package mil.nga.giat.geowave.examples.query;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.VectorDataStore;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.ArrayList;

/**
 * This class is intended to provide a few examples on running Geowave queries of different types:
 * 1- Querying by polygon a set of points.
 * 2- Filtering on attributes of features using CQL queries
 * 3- Ingesting polygons, and running intersect queries - polygon intersects polygons.
 * You can check all points, geometries and query accuracy in a more visual manner @ http://geojson.io/
 */
public class SpatialQueryExample {
    //We'll use GeoWave's VectorDataStore, which allows to run CQL rich queries
    private static VectorDataStore dataStore;
    //We need the AccumuloAdapterStore, which keeps a registry of adapter-ids, used to be able to query specific "tables" or types of features.
    private static AccumuloAdapterStore adapterStore;


    public static void main(String[] args) throws AccumuloSecurityException, AccumuloException, ParseException {
        SpatialQueryExample example = new SpatialQueryExample();
        example.setupDataStores();
        example.runPointExamples();
        example.runPolygonExamples();
    }

    private void setupDataStores() throws AccumuloSecurityException, AccumuloException {
        //Initialize VectorDataStore and AccumuloAdapterStore
        MockInstance instance = new MockInstance();
        //For the MockInstance we can user "user" - "password" as our connection tokens
        Connector connector = instance.getConnector("user", new PasswordToken("password"));
        BasicAccumuloOperations operations = new BasicAccumuloOperations(connector);
        adapterStore = new AccumuloAdapterStore(operations);
        dataStore = new VectorDataStore(operations);
    }

    /**
     * We'll run our point related operations.
     * The data ingested and queried is single point based, meaning the index constructed will be based on a point.
     */
    private void runPointExamples() {
        ingestPointData();
    }

    private void ingestPointData() {
        ingestPointBasicFeature();
        ingestPointComplexFeature();
    }

    private void ingestPointBasicFeature() {
        //First, we'll build our first kind of SimpleFeature, which we'll call "basic-feature"
        //We need the type builder to build the feature type
        SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
        //AttributeTypeBuilder for the attributes of the SimpleFeature
        AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();
        //Here we're setting the SimpleFeature name. Later on, we'll be able to query GW just by this particular feature.
        sftBuilder.setName("basic-feature");
        //Add the attributes to the feature
        //Add the geometry attribute, which is mandatory for GeoWave to be able to construct an index out of the SimpleFeature
        sftBuilder.add(attrBuilder.binding(Point.class).nillable(false).buildDescriptor("geometry"));
        //Add another attribute just to be able to filter by it in CQL
        sftBuilder.add(attrBuilder.binding(String.class).nillable(false).buildDescriptor("filter"));

        //Create the SimpleFeatureType
        SimpleFeatureType sfType = sftBuilder.buildFeatureType();
        //We need the adapter for all our operations with GeoWave
        FeatureDataAdapter sfAdapter = new FeatureDataAdapter(sfType);

        //Now we build the actual features. We'll create two points.
        //First point
        SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(sfType);
        sfBuilder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(-80.211181640625, 25.848101000701597)));
        sfBuilder.set("filter", "Basic-Stadium");
        //When calling buildFeature, we need to pass an unique id for that feature, or it will be overwritten.
        SimpleFeature basicPoint1 = sfBuilder.buildFeature("1");

        //Construct the second feature.
        sfBuilder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(-80.191360, 25.777804)));
        sfBuilder.set("filter", "Basic-College");
        SimpleFeature basicPoint2 = sfBuilder.buildFeature("2");

        ArrayList<SimpleFeature> features = new ArrayList<SimpleFeature>();
        features.add(basicPoint1);
        features.add(basicPoint2);

        //Ingest the data. For that purpose, we need the feature adapter,
        // the index type (the default spatial index is used here),
        // and an iterator of SimpleFeature
        dataStore.ingest(sfAdapter, IndexType.SPATIAL_VECTOR.createDefaultIndex(), features.iterator());
    }

    /**
     * We're going to ingest a more complete simple feature.
     */
    private void ingestPointComplexFeature() {
        //First, we'll build our second kind of SimpleFeature, which we'll call "complex-feature"
        //We need the type builder to build the feature type
        SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
        //AttributeTypeBuilder for the attributes of the SimpleFeature
        AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();
        //Here we're setting the SimpleFeature name. Later on, we'll be able to query GW just by this particular feature.
        sftBuilder.setName("complex-feature");
        //Add the attributes to the feature
        //Add the geometry attribute, which is mandatory for GeoWave to be able to construct an index out of the SimpleFeature
        sftBuilder.add(attrBuilder.binding(Point.class).nillable(false).buildDescriptor("geometry"));
        //Add another attribute just to be able to filter by it in CQL
        sftBuilder.add(attrBuilder.binding(String.class).nillable(false).buildDescriptor("filter"));
        //Add more attributes to use with CQL filtering later on.
        sftBuilder.add(attrBuilder.binding(Double.class).nillable(false).buildDescriptor("latitude"));
        sftBuilder.add(attrBuilder.binding(Double.class).nillable(false).buildDescriptor("longitude"));

        //Create the SimpleFeatureType
        SimpleFeatureType sfType = sftBuilder.buildFeatureType();
        //We need the adapter for all our operations with GeoWave
        FeatureDataAdapter sfAdapter = new FeatureDataAdapter(sfType);

        //Now we build the actual features. We'll create two more points.
        //First point
        SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(sfType);
        sfBuilder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(-80.193388, 25.780538)));
        sfBuilder.set("filter", "Complex-Station");
        sfBuilder.set("latitude", 25.780538);
        sfBuilder.set("longitude", -80.193388);
        //When calling buildFeature, we need to pass an unique id for that feature, or it will be overwritten.
        SimpleFeature basicPoint1 = sfBuilder.buildFeature("1");

        //Construct the second feature.
        sfBuilder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(-118.26713562011719, 33.988349152677955)));
        sfBuilder.set("filter", "Complex-LA");
        sfBuilder.set("latitude", 33.988349152677955);
        sfBuilder.set("longitude", -118.26713562011719);
        SimpleFeature basicPoint2 = sfBuilder.buildFeature("2");

        ArrayList<SimpleFeature> features = new ArrayList<SimpleFeature>();
        features.add(basicPoint1);
        features.add(basicPoint2);

        //Ingest the data. For that purpose, we need the feature adapter,
        // the index type (the default spatial index is used here),
        // and an iterator of SimpleFeature
        dataStore.ingest(sfAdapter, IndexType.SPATIAL_VECTOR.createDefaultIndex(), features.iterator());

        /**
         * After ingest, a single point might look like this in Accumulo.
         */
//        \x1F\x11\xCB\xFC\xB6\xEFT\x00\xFFcomplex_feature4\x00\x00\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x00 complex_feature:filter []    Complex-LA
//        \x1F\x11\xCB\xFC\xB6\xEFT\x00\xFFcomplex_feature4\x00\x00\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x00 complex_feature:geom\x00\x00 []    \x00\x00\x00\x00\x01\xC0]\x91\x18\xC0\x00\x00\x00@@\xFE\x829\x9B\xE3\xFC
//        \x1F\x11\xCB\xFC\xB6\xEFT\x00\xFFcomplex_feature4\x00\x00\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x00 complex_feature:latitude []    @@\xFE\x829\x9B\xE3\xFC
//        \x1F\x11\xCB\xFC\xB6\xEFT\x00\xFFcomplex_feature\x00\x00\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x00 complex_feature:longitude []    \xC0]\x91\x18\xC0\x00\x00\x
    }

    /**
     * We'll run our polygon related operations.
     * The data ingested and queried is single polygon based, meaning the index constructed will be based on a Geometry.
     */
    private void runPolygonExamples() throws ParseException {
        ingestPolygonFeature();
    }

    private void ingestPolygonFeature() throws ParseException {
        //First, we'll build our third kind of SimpleFeature, which we'll call "polygon-feature"
        //We need the type builder to build the feature type
        SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
        //AttributeTypeBuilder for the attributes of the SimpleFeature
        AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();
        //Here we're setting the SimpleFeature name. Later on, we'll be able to query GW just by this particular feature.
        sftBuilder.setName("polygon-feature");
        //Add the attributes to the feature
        //Add the geometry attribute, which is mandatory for GeoWave to be able to construct an index out of the SimpleFeature
        //Will be any arbitrary geometry; in this case, a polygon.
        sftBuilder.add(attrBuilder.binding(Geometry.class).nillable(false).buildDescriptor("geometry"));
        //Add another attribute just to be able to filter by it in CQL
        sftBuilder.add(attrBuilder.binding(String.class).nillable(false).buildDescriptor("filter"));

        //Create the SimpleFeatureType
        SimpleFeatureType sfType = sftBuilder.buildFeatureType();
        //We need the adapter for all our operations with GeoWave
        FeatureDataAdapter sfAdapter = new FeatureDataAdapter(sfType);

        //Now we build the actual features. We'll create one polygon.
        //First point
        SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(sfType);


        //For ease of use, we'll create the polygon geometry with WKT format.
        String polygonDefinition = "POLYGON (( " +
                "-80.3045654296875 25.852426562716428, " +
                "-80.123291015625 25.808545671771615, " +
                "-80.19195556640625 25.7244467526159, " +
                "-80.34233093261719 25.772068899816585, " +
                "-80.3045654296875 25.852426562716428" +
                "))";
        Geometry geom = new WKTReader(JTSFactoryFinder.getGeometryFactory()).read(polygonDefinition));
        sfBuilder.set("geometry", geom);
        sfBuilder.set("filter", "Polygon");
        //When calling buildFeature, we need to pass an unique id for that feature, or it will be overwritten.
        SimpleFeature polygon = sfBuilder.buildFeature("1");

        ArrayList<SimpleFeature> features = new ArrayList<SimpleFeature>();
        features.add(polygon);

        //Ingest the data. For that purpose, we need the feature adapter,
        // the index type (the default spatial index is used here),
        // and an iterator of SimpleFeature
        dataStore.ingest(sfAdapter, IndexType.SPATIAL_VECTOR.createDefaultIndex(), features.iterator());
    }
}

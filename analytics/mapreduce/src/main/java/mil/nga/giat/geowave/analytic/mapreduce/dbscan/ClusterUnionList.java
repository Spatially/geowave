package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.util.Map;

import mil.nga.giat.geowave.analytic.GeometryHullTool;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.nn.DistanceProfile;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NeighborList;
import mil.nga.giat.geowave.analytic.mapreduce.nn.NeighborListFactory;
import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * 
 * A cluster represented by a hull.
 * 
 * Intended to run in a single thread. Not Thread Safe.
 * 
 * 
 * TODO: connectGeometryTool.connect(
 */
public class ClusterUnionList extends
		DBScanClusterList implements
		CompressingCluster<ClusterItem, Geometry>
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(ClusterUnionList.class);

	public ClusterUnionList(
			final GeometryHullTool connectGeometryTool,
			final ByteArrayId centerId,
			final ClusterItem center,
			final NeighborListFactory<ClusterItem> factory,
			final Map<ByteArrayId, Cluster<ClusterItem>> index ) {
		super(
				1,
				connectGeometryTool,
				centerId,
				index);
		super.clusterGeo = center.getGeometry();
	}

	protected long addAndFetchCount(
			final ByteArrayId id,
			final ClusterItem newInstance,
			final DistanceProfile<?> distanceProfile ) {
		union(newInstance.getGeometry());
		return (Long) newInstance.getCount();
	}

	@Override
	public void merge(
			final Cluster<ClusterItem> cluster ) {
		super.merge(cluster);
		if (cluster != this) {
			union(((DBScanClusterList) cluster).clusterGeo);
		}
	}

	public boolean isCompressed() {
		return true;
	}

	protected Geometry compress() {
		return clusterGeo;
	}

	public static class ClusterUnionListFactory implements
			NeighborListFactory<ClusterItem>
	{
		private final Map<ByteArrayId, Cluster<ClusterItem>> index;
		protected final GeometryHullTool connectGeometryTool = new GeometryHullTool();

		public ClusterUnionListFactory(
				final DistanceFn<Coordinate> distanceFnForCoordinate,
				final Map<ByteArrayId, Cluster<ClusterItem>> index ) {
			super();
			connectGeometryTool.setDistanceFnForCoordinate(distanceFnForCoordinate);
			this.index = index;
		}

		public NeighborList<ClusterItem> buildNeighborList(
				final ByteArrayId centerId,
				final ClusterItem center ) {
			Cluster<ClusterItem> list = index.get(centerId);
			if (list == null) {
				list = new ClusterUnionList(
						connectGeometryTool,
						centerId,
						center,
						this,
						index);
			}
			return list;
		}
	}
}

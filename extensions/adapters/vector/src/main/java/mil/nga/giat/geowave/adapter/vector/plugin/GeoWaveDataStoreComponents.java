package mil.nga.giat.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.TransactionsAllocator;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.opengis.feature.simple.SimpleFeature;

public class GeoWaveDataStoreComponents
{
	private final FeatureDataAdapter adapter;
	private final DataStore dataStore;
	private final IndexStore indexStore;
	private final DataStatisticsStore dataStatisticsStore;
	private final GeoWaveGTDataStore gtStore;
	private final TransactionsAllocator transactionAllocator;

	private final Index currentIndex;

	public GeoWaveDataStoreComponents(
			final DataStore dataStore,
			final DataStatisticsStore dataStatisticsStore,
			final IndexStore indexStore,
			final FeatureDataAdapter adapter,
			final GeoWaveGTDataStore gtStore,
			final TransactionsAllocator transactionAllocator ) {
		this.adapter = adapter;
		this.dataStore = dataStore;
		this.indexStore = indexStore;
		this.dataStatisticsStore = dataStatisticsStore;
		this.gtStore = gtStore;
		currentIndex = gtStore.getIndex(adapter);
		this.transactionAllocator = transactionAllocator;
	}

	public IndexStore getIndexStore() {
		return indexStore;
	}

	public FeatureDataAdapter getAdapter() {
		return adapter;
	}

	public DataStore getDataStore() {
		return dataStore;
	}

	public GeoWaveGTDataStore getGTstore() {
		return gtStore;
	}

	public Index getCurrentIndex() {
		return currentIndex;
	}

	@SuppressWarnings("unchecked")
	public Map<ByteArrayId, DataStatistics<SimpleFeature>> getDataStatistics(
			final GeoWaveTransaction transaction ) {
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats = new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>();

		for (final ByteArrayId statsId : adapter.getSupportedStatisticsIds()) {
			@SuppressWarnings("unused")
			final DataStatistics<SimpleFeature> put = stats.put(
					statsId,
					(DataStatistics<SimpleFeature>) dataStatisticsStore.getDataStatistics(
							adapter.getAdapterId(),
							statsId,
							transaction.composeAuthorizations()));
		}
		return stats;
	}

	public void remove(
			final SimpleFeature feature,
			final GeoWaveTransaction transaction )
			throws IOException {

		dataStore.delete(
				new QueryOptions(
						adapter,
						currentIndex,
						transaction.composeAuthorizations()),
				new DataIdQuery(
						adapter.getAdapterId(),
						adapter.getDataId(feature)));
	}

	public void remove(
			final String fid,
			final GeoWaveTransaction transaction )
			throws IOException {

		dataStore.delete(
				new QueryOptions(
						adapter,
						currentIndex,
						transaction.composeAuthorizations()),
				new DataIdQuery(
						new ByteArrayId(
								StringUtils.stringToBinary(fid)),
						adapter.getAdapterId()));

	}

	@SuppressWarnings("unchecked")
	public void write(
			final Iterator<SimpleFeature> featureIt,
			final Set<String> fidList,
			final GeoWaveTransaction transaction )
			throws IOException {
		try (IndexWriter indexWriter = dataStore.createIndexWriter(
				currentIndex,
				new UniformVisibilityWriter<SimpleFeature>(
						new GlobalVisibilityHandler(
								transaction.composeVisibility())))) {
			while (featureIt.hasNext()) {
				final SimpleFeature feature = featureIt.next();
				fidList.add(feature.getID());
				indexWriter.write(
						adapter,
						feature);
			}
		}
	}

	public List<ByteArrayId> writeCommit(
			final SimpleFeature feature,
			final GeoWaveTransaction transaction )
			throws IOException {
		try (IndexWriter indexWriter = dataStore.createIndexWriter(
				currentIndex,
				new UniformVisibilityWriter<SimpleFeature>(
						new GlobalVisibilityHandler(
								transaction.composeVisibility())))) {
			return indexWriter.write(
					adapter,
					feature);
		}
	}

	public String getTransaction()
			throws IOException {
		return transactionAllocator.getTransaction();
	}

	public void releaseTransaction(
			final String txID )
			throws IOException {
		transactionAllocator.releaseTransaction(txID);
	}
}

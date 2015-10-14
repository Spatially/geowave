package mil.nga.giat.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.auth.AuthorizationSPI;
import mil.nga.giat.geowave.adapter.vector.plugin.lock.LockingManagement;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveAutoCommitTransactionState;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransactionManagementState;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransactionState;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.MemoryTransactionsAllocator;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.TransactionsAllocator;
import mil.nga.giat.geowave.adapter.vector.plugin.visibility.ColumnVisibilityManagement;
import mil.nga.giat.geowave.adapter.vector.plugin.visibility.VisibilityManagementHelper;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.query.AdapterIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.apache.log4j.Logger;
import org.geotools.data.FeatureListenerManager;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class GeoWaveGTDataStore extends
		ContentDataStore
{
	/** Package logger */
	private final static Logger LOGGER = Logger.getLogger(GeoWaveGTDataStore.class);
	public static final CoordinateReferenceSystem DEFAULT_CRS;

	static {
		try {
			DEFAULT_CRS = CRS.decode(
					"EPSG:4326",
					true);
		}
		catch (final FactoryException e) {
			LOGGER.error(
					"Unable to decode EPSG:4326 CRS",
					e);
			throw new RuntimeException(
					"Unable to initialize EPSG:4326 object",
					e);
		}
	}

	private FeatureListenerManager listenerManager = null;
	protected AdapterStore adapterStore;
	protected IndexStore indexStore;
	protected DataStatisticsStore dataStatisticsStore;
	protected DataStore dataStore;
	private final Map<String, Index> preferredIndexes = new ConcurrentHashMap<String, Index>();
	private final ColumnVisibilityManagement<SimpleFeature> visibilityManagement = VisibilityManagementHelper.loadVisibilityManagement();
	private final AuthorizationSPI authorizationSPI;
	private final URI featureNameSpaceURI;
	private int transactionBufferSize = 10000;
	private final TransactionsAllocator transactionsAllocator;

	public GeoWaveGTDataStore(
			final GeoWavePluginConfig config )
			throws IOException {
		listenerManager = new FeatureListenerManager();
		lockingManager = config.getLockingManagementFactory().createLockingManager(
				config);
		authorizationSPI = config.getAuthorizationFactory().create(
				config.getAuthorizationURL());
		init(config);
		featureNameSpaceURI = config.getFeatureNamespace();
		transactionBufferSize = config.getTransactionBufferSize();
		transactionsAllocator = new MemoryTransactionsAllocator();

	}

	public void init(
			final GeoWavePluginConfig config ) {
		dataStore = config.getDataStore();
		dataStatisticsStore = config.getDataStatisticsStore();
		indexStore = config.getIndexStore();
		adapterStore = config.getAdapterStore();
	}

	public AuthorizationSPI getAuthorizationSPI() {
		return authorizationSPI;
	}

	public FeatureListenerManager getListenerManager() {
		return listenerManager;
	}

	public DataStore getDataStore() {
		return dataStore;
	}

	public AdapterStore getAdapterStore() {
		return adapterStore;
	}

	public IndexStore getIndexStore() {
		return indexStore;
	}

	public DataStatisticsStore getDataStatisticsStore() {
		return dataStatisticsStore;
	}

	protected Index getIndex(
			final FeatureDataAdapter adapter ) {
		return getPreferredIndex(adapter);
	}

	@Override
	public void createSchema(
			final SimpleFeatureType featureType ) {
		if (featureType.getGeometryDescriptor() == null) {
			throw new UnsupportedOperationException(
					"Schema missing geometry");
		}
		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				featureType,
				visibilityManagement);
		if (featureNameSpaceURI != null) {
			adapter.setNamespace(featureNameSpaceURI.toString());
		}

		adapterStore.addAdapter(adapter);
		getPreferredIndex(adapter);
	}

	private FeatureDataAdapter getAdapter(
			final String typeName ) {
		final FeatureDataAdapter featureAdapter;
		final DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
				StringUtils.stringToBinary(typeName)));
		if ((adapter == null) || !(adapter instanceof FeatureDataAdapter)) {
			return null;
		}
		featureAdapter = (FeatureDataAdapter) adapter;
		if (featureNameSpaceURI != null) {
			featureAdapter.setNamespace(featureNameSpaceURI.toString());
		}
		return featureAdapter;
	}

	@Override
	protected List<Name> createTypeNames()
			throws IOException {
		final List<Name> names = new ArrayList<>();
		final CloseableIterator<DataAdapter<?>> adapters = adapterStore.getAdapters();
		while (adapters.hasNext()) {
			final DataAdapter<?> adapter = adapters.next();
			if (adapter instanceof FeatureDataAdapter) {
				names.add(new NameImpl(
						((FeatureDataAdapter) adapter).getType().getTypeName()));
			}
		}
		adapters.close();
		return names;
	}

	@Override
	public ContentFeatureSource getFeatureSource(
			final String typeName )
			throws IOException {
		return getFeatureSource(
				typeName,
				Transaction.AUTO_COMMIT);
	}

	@Override
	public ContentFeatureSource getFeatureSource(
			final String typeName,
			final Transaction tx )
			throws IOException {
		return super.getFeatureSource(
				new NameImpl(
						null,
						typeName),
				tx);
	}

	@Override
	public ContentFeatureSource getFeatureSource(
			final Name typeName,
			final Transaction tx )
			throws IOException {
		return getFeatureSource(
				typeName.getLocalPart(),
				tx);

	}

	@Override
	public ContentFeatureSource getFeatureSource(
			final Name typeName )
			throws IOException {
		return getFeatureSource(
				typeName.getLocalPart(),
				Transaction.AUTO_COMMIT);
	}

	@Override
	protected ContentFeatureSource createFeatureSource(
			final ContentEntry entry )
			throws IOException {
		return new GeoWaveFeatureSource(
				entry,
				Query.ALL,
				getAdapter(entry.getTypeName()),
				transactionsAllocator);
	}

	@Override
	public void removeSchema(
			final Name typeName )
			throws IOException {
		this.removeSchema(typeName.getLocalPart());
	}

	@Override
	public void removeSchema(
			final String typeName )
			throws IOException {
		final DataAdapter<?> adapter = adapterStore.getAdapter(new ByteArrayId(
				StringUtils.stringToBinary(typeName)));
		if (adapter != null) {
			final String[] authorizations = getAuthorizationSPI().getAuthorizations();
			dataStore.delete(
					new QueryOptions(
							adapter,
							(Index) null,
							authorizations),
					new AdapterIdQuery(
							adapter.getAdapterId()));
			// TODO do we want to delete the adapter from the adapter store?
		}
	}

	/**
	 * Used to retrieve the TransactionStateDiff for this transaction.
	 * <p>
	 * 
	 * @param transaction
	 * @return GeoWaveTransactionState or null if subclass is handling
	 *         differences
	 * @throws IOException
	 */
	protected GeoWaveTransactionState getMyTransactionState(
			final Transaction transaction,
			final GeoWaveFeatureSource source )
			throws IOException {
		synchronized (transaction) {
			GeoWaveTransactionState state = null;
			if (transaction == Transaction.AUTO_COMMIT) {
				state = new GeoWaveAutoCommitTransactionState(
						source);
			}
			else {
				state = (GeoWaveTransactionState) transaction.getState(this);
				if (state == null) {
					state = new GeoWaveTransactionManagementState(
							transactionBufferSize,
							source.getComponents(),
							transaction,
							(LockingManagement) lockingManager);
					transaction.putState(
							this,
							state);
				}
			}
			return state;
		}
	}

	private Index getPreferredIndex(
			final FeatureDataAdapter adapter ) {

		Index currentSelection = preferredIndexes.get(adapter.getType().getName().toString());
		if (currentSelection != null) {
			return currentSelection;
		}

		final boolean needTime = adapter.hasTemporalConstraints();

		try (CloseableIterator<Index> indices = indexStore.getIndices()) {
			boolean currentSelectionHasTime = false;
			while (indices.hasNext()) {
				final Index index = indices.next();
				@SuppressWarnings("rawtypes")
				final DimensionField[] dims = index.getIndexModel().getDimensions();
				boolean hasLat = false;
				boolean hasLong = false;
				boolean hasTime = false;
				for (final DimensionField<?> dim : dims) {
					hasLat |= dim instanceof LatitudeField;
					hasLong |= dim instanceof LongitudeField;
					hasTime |= dim instanceof TimeField;
				}

				// pick the first matching one or
				// pick the one does not match the required time constraints
				if (hasLat && hasLong) {
					if ((currentSelection == null) || (currentSelectionHasTime != needTime)) {
						currentSelection = index;
						currentSelectionHasTime = hasTime;
					}
				}
			}
			// at this point, preferredID is not found
			// only select the index if one has not been found or
			// the current selection. Not using temporal at this point.
			// temporal index should only be used if explicitly requested.
			if (currentSelection == null) {
				currentSelection = IndexType.SPATIAL_VECTOR.createDefaultIndex();
			}
		}
		catch (final IOException ex) {
			LOGGER.error(
					"Cannot close index iterator.",
					ex);
		}

		preferredIndexes.put(
				adapter.getType().getName().toString(),
				currentSelection);
		return currentSelection;
	}
}

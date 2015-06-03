package mil.nga.giat.geowave.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.mapreduce.JobContextIndexStore;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputConfigurator.InputConfig;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Transformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

public class GeoWaveInputFormat<T> extends
		InputFormat<GeoWaveInputKey, T>
{
	private static final Class<?> CLASS = GeoWaveInputFormat.class;
	protected static final Logger LOGGER = Logger.getLogger(CLASS);

	/**
	 * Add an adapter specific to the input format
	 *
	 * @param job
	 * @param adapter
	 */
	public static void addDataAdapter(
			final Configuration config,
			final DataAdapter<?> adapter ) {

		// Also store for use the mapper and reducers
		JobContextAdapterStore.addDataAdapter(
				config,
				adapter);
		GeoWaveConfiguratorBase.addDataAdapter(
				CLASS,
				config,
				adapter);
	}

	public static void setDataStoreName(
			final Configuration config,
			final String dataStoreName ) {
		GeoWaveConfiguratorBase.setDataStoreName(
				CLASS,
				config,
				dataStoreName);
	}

	public static void setGeoWaveNamespace(
			final Configuration config,
			final String namespace ) {
		GeoWaveConfiguratorBase.setGeoWaveNamespace(
				CLASS,
				config,
				namespace);
	}

	public static void setStoreConfigOptions(
			final Configuration config,
			final Map<String, String> storeConfigOptions ) {
		GeoWaveConfiguratorBase.setStoreConfigOptions(
				CLASS,
				config,
				storeConfigOptions);
	}

	public static IndexStore getJobContextIndexStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextIndexStore(
				CLASS,
				context);
	}

	public static AdapterStore getJobContextAdapterStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextAdapterStore(
				CLASS,
				context);
	}

	public static void addIndex(
			final Configuration config,
			final Index index ) {
		JobContextIndexStore.addIndex(
				config,
				index);
	}

	public static void setMinimumSplitCount(
			final Configuration config,
			final Integer minSplits ) {
		GeoWaveInputConfigurator.setMinimumSplitCount(
				CLASS,
				config,
				minSplits);
	}

	public static void setMaximumSplitCount(
			final Configuration config,
			final Integer maxSplits ) {
		GeoWaveInputConfigurator.setMaximumSplitCount(
				CLASS,
				config,
				maxSplits);
	}

	public static void setIsOutputWritable(
			final Configuration config,
			final Boolean isOutputWritable ) {
		config.setBoolean(
				GeoWaveConfiguratorBase.enumToConfKey(
						CLASS,
						InputConfig.OUTPUT_WRITABLE),
				isOutputWritable);
	}

	public static void setQuery(
			final Configuration config,
			final DistributableQuery query ) {
		GeoWaveInputConfigurator.setQuery(
				CLASS,
				config,
				query);
	}

	protected static DistributableQuery getQuery(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getQuery(
				CLASS,
				context);
	}

	protected static Index[] getIndices(
			final JobContext context ) {
		return GeoWaveInputConfigurator.searchForIndices(
				CLASS,
				context);
	}

	protected static String getGeoWaveNamespace(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getGeoWaveNamespace(
				CLASS,
				context);
	}

	protected static Boolean isOutputWritable(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getConfiguration(
				context).getBoolean(
				GeoWaveConfiguratorBase.enumToConfKey(
						CLASS,
						InputConfig.OUTPUT_WRITABLE),
				false);
	}

	protected static Integer getMinimumSplitCount(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getMinimumSplitCount(
				CLASS,
				context);
	}

	protected static Integer getMaximumSplitCount(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getMaximumSplitCount(
				CLASS,
				context);
	}

	public static void addAuthorization(
			final Configuration config,
			final String authorization ) {
		GeoWaveInputConfigurator.addAuthorization(
				CLASS,
				config,
				authorization);
	}

	@Override
	public RecordReader<GeoWaveInputKey, T> createRecordReader(
			final InputSplit split,
			final TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		final Map<String, Object> configOptions = ConfigUtils.valuesFromStrings(getStoreConfigOptions(context));
		final String namespace = getGeoWaveNamespace(context);
		final DataStore dataStore = GeoWaveStoreFinder.createDataStore(
				configOptions,
				namespace);
		final AdapterStore adapterStore = getJobContextAdapterStore(context);
		if ((dataStore != null) && (dataStore instanceof MapReduceDataStore)) {
			return (RecordReader<GeoWaveInputKey, T>) ((MapReduceDataStore) dataStore).createRecordReader(
					getIndices(context),
					getAdapterIds(
							context,
							adapterStore),
					getQuery(context),
					adapterStore,
					getJobContextIndexStore(context),
					isOutputWritable(context),
					getAuthorizations(context),
					split);
		}
		LOGGER.error("Data Store does not support map reduce");
		throw new IOException(
				"Data Store does not support map reduce");
	}

	/**
	 * Check whether a configuration is fully configured to be used with an
	 * Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
	 *
	 * @param context
	 *            the Hadoop context for the configured job
	 * @throws IOException
	 *             if the context is improperly configured
	 * @since 1.5.0
	 */
	protected static void validateOptions(
			final JobContext context )
			throws IOException {// attempt to get each of the GeoWave stores
								// from the job context
		try {
			final String namespace = getGeoWaveNamespace(context);

			final Map<String, Object> configOptions = ConfigUtils.valuesFromStrings(getStoreConfigOptions(context));
			if (GeoWaveStoreFinder.createDataStore(
					configOptions,
					namespace) == null) {
				final String msg = "Unable to find GeoWave data store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
			if (GeoWaveStoreFinder.createIndexStore(
					configOptions,
					namespace) == null) {
				final String msg = "Unable to find GeoWave index store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
			if (GeoWaveStoreFinder.createAdapterStore(
					configOptions,
					namespace) == null) {
				final String msg = "Unable to find GeoWave adapter store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
			if (GeoWaveStoreFinder.createDataStatisticsStore(
					configOptions,
					namespace) == null) {
				final String msg = "Unable to find GeoWave data statistics store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Error finding GeoWave stores",
					e);
			throw new IOException(
					"Error finding GeoWave stores",
					e);
		}
	}

	public static Map<String, String> getStoreConfigOptions(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getStoreConfigOptions(
				CLASS,
				context);
	}

	protected static String[] getAuthorizations(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getAuthorizations(
				CLASS,
				context);
	}

	/**
	 * First look for input-specific adapters
	 *
	 * @param context
	 * @param adapterStore
	 * @return
	 */
	public static List<ByteArrayId> getAdapterIds(
			final JobContext context,
			final AdapterStore adapterStore ) {
		final DataAdapter<?>[] userAdapters = GeoWaveConfiguratorBase.getDataAdapters(
				CLASS,
				context);
		if ((userAdapters == null) || (userAdapters.length <= 0)) {
			return IteratorUtils.toList(IteratorUtils.transformedIterator(
					adapterStore.getAdapters(),
					new Transformer() {

						@Override
						public Object transform(
								final Object input ) {
							if (input instanceof DataAdapter) {
								return ((DataAdapter) input).getAdapterId();
							}
							return input;
						}
					}));
		}
		else {
			final List<ByteArrayId> retVal = new ArrayList<ByteArrayId>(
					userAdapters.length);
			for (final DataAdapter<?> adapter : userAdapters) {
				retVal.add(adapter.getAdapterId());
			}
			return retVal;
		}
	}

	@Override
	public List<InputSplit> getSplits(
			final JobContext context )
			throws IOException,
			InterruptedException {
		final Map<String, Object> configOptions = ConfigUtils.valuesFromStrings(getStoreConfigOptions(context));
		final String namespace = getGeoWaveNamespace(context);
		final DataStore dataStore = GeoWaveStoreFinder.createDataStore(
				configOptions,
				namespace);
		final AdapterStore adapterStore = getJobContextAdapterStore(context);
		if ((dataStore != null) && (dataStore instanceof MapReduceDataStore)) {
			return ((MapReduceDataStore) dataStore).getSplits(
					getIndices(context),
					getAdapterIds(
							context,
							adapterStore),
					getQuery(context),
					adapterStore,
					getJobContextIndexStore(context),
					getAuthorizations(context),
					getMinimumSplitCount(context),
					getMaximumSplitCount(context));
		}
		LOGGER.error("Data Store does not support map reduce");
		throw new IOException(
				"Data Store does not support map reduce");
	}
}
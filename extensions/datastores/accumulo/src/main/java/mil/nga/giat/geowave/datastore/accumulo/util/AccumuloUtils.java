package mil.nga.giat.geowave.datastore.accumulo.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.FilterList;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig.OptionProvider;
import mil.nga.giat.geowave.datastore.accumulo.RowMergingAdapterOptionProvider;
import mil.nga.giat.geowave.datastore.accumulo.RowMergingCombiner;
import mil.nga.giat.geowave.datastore.accumulo.RowMergingVisibilityCombiner;
import mil.nga.giat.geowave.datastore.accumulo.Writer;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AbstractAccumuloPersistence;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloConstraintsQuery;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * A set of convenience methods for common operations on Accumulo within
 * GeoWave, such as conversions between GeoWave objects and corresponding
 * Accumulo objects.
 * 
 */
public class AccumuloUtils
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloUtils.class);
	public final static String ALT_INDEX_TABLE = "_GEOWAVE_ALT_INDEX";
	private static final String ROW_MERGING_SUFFIX = "_COMBINER";
	private static final String ROW_MERGING_VISIBILITY_SUFFIX = "_VISIBILITY_COMBINER";

	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	private static final UniformVisibilityWriter DEFAULT_VISIBILITY = new UniformVisibilityWriter(
			new UnconstrainedVisibilityHandler());

	public static Range byteArrayRangeToAccumuloRange(
			final ByteArrayRange byteArrayRange ) {
		final Text start = new Text(
				byteArrayRange.getStart().getBytes());
		final Text end = new Text(
				byteArrayRange.getEnd().getBytes());
		if (start.compareTo(end) > 0) {
			return null;
		}
		return new Range(
				new Text(
						byteArrayRange.getStart().getBytes()),
				true,
				Range.followingPrefix(new Text(
						byteArrayRange.getEnd().getBytes())),
				false);
	}

	public static TreeSet<Range> byteArrayRangesToAccumuloRanges(
			final List<ByteArrayRange> byteArrayRanges ) {
		if (byteArrayRanges == null) {
			final TreeSet<Range> range = new TreeSet<Range>();
			range.add(new Range());
			return range;
		}
		final TreeSet<Range> accumuloRanges = new TreeSet<Range>();
		for (final ByteArrayRange byteArrayRange : byteArrayRanges) {
			final Range range = byteArrayRangeToAccumuloRange(byteArrayRange);
			if (range == null) {
				continue;
			}
			accumuloRanges.add(range);
		}
		if (accumuloRanges.isEmpty()) {
			// implies full table scan
			accumuloRanges.add(new Range());
		}
		return accumuloRanges;
	}

	public static String getQualifiedTableName(
			final String tableNamespace,
			final String unqualifiedTableName ) {
		return ((tableNamespace == null) || tableNamespace.isEmpty()) ? unqualifiedTableName : tableNamespace + "_" + unqualifiedTableName;
	}

	public static Object decodeRow(
			final Key key,
			final Value value,
			final DataAdapter<?> adapter,
			final Index index ) {
		return decodeRow(
				key,
				value,
				adapter,
				null,
				index);
	}

	public static Object decodeRow(
			final Key key,
			final Value value,
			final DataAdapter<?> adapter,
			final QueryFilter clientFilter,
			final Index index ) {
		final AccumuloRowId rowId = new AccumuloRowId(
				key.getRow().copyBytes());
		return decodeRowObj(
				key,
				value,
				rowId,
				adapter,
				null,
				clientFilter,
				index,
				null);
	}

	@SuppressWarnings("unchecked")
	public static <T> T decodeRow(
			final Key key,
			final Value value,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final Index index,
			final ScanCallback<T> scanCallback ) {
		final AccumuloRowId rowId = new AccumuloRowId(
				key.getRow().copyBytes());
		return (T) decodeRowObj(
				key,
				value,
				rowId,
				null,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
	}

	public static Object decodeRow(
			final Key key,
			final Value value,
			final AccumuloRowId rowId,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final Index index ) {
		return decodeRowObj(
				key,
				value,
				rowId,
				null,
				adapterStore,
				clientFilter,
				index,
				null);
	}

	private static <T> Object decodeRowObj(
			final Key key,
			final Value value,
			final AccumuloRowId rowId,
			final DataAdapter<T> dataAdapter,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final Index index,
			final ScanCallback<T> scanCallback ) {
		final Pair<T, DataStoreEntryInfo> pair = decodeRow(
				key,
				value,
				rowId,
				dataAdapter,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
		return pair != null ? pair.getLeft() : null;

	}

	@SuppressWarnings("unchecked")
	public static <T> Pair<T, DataStoreEntryInfo> decodeRow(
			final Key k,
			final Value v,
			final AccumuloRowId rowId,
			final DataAdapter<T> dataAdapter,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final Index index,
			final ScanCallback<T> scanCallback ) {
		if ((dataAdapter == null) && (adapterStore == null)) {
			LOGGER.error("Could not decode row from iterator. Either adapter or adapter store must be non-null.");
			return null;
		}
		DataAdapter<T> adapter = dataAdapter;
		SortedMap<Key, Value> rowMapping;
		try {
			rowMapping = WholeRowIterator.decodeRow(
					k,
					v);
		}
		catch (final IOException e) {
			LOGGER.error("Could not decode row from iterator. Ensure whole row iterators are being used.");
			return null;
		}
		// build a persistence encoding object first, pass it through the
		// client filters and if its accepted, use the data adapter to
		// decode the persistence model into the native data type
		final PersistentDataset<CommonIndexValue> indexData = new PersistentDataset<CommonIndexValue>();
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
		final PersistentDataset<byte[]> unknownData = new PersistentDataset<byte[]>();
		// for now we are assuming all entries in a row are of the same type
		// and use the same adapter
		boolean adapterMatchVerified;
		ByteArrayId adapterId;
		if (adapter != null) {
			adapterId = adapter.getAdapterId();
			adapterMatchVerified = false;
		}
		else {
			adapterMatchVerified = true;
			adapterId = null;
		}
		final List<FieldInfo> fieldInfoList = new ArrayList<FieldInfo>(
				rowMapping.size());

		for (final Entry<Key, Value> entry : rowMapping.entrySet()) {
			// the column family is the data element's type ID
			if (adapterId == null) {
				adapterId = new ByteArrayId(
						entry.getKey().getColumnFamilyData().getBackingArray());
			}

			if (adapter == null) {
				adapter = (DataAdapter<T>) adapterStore.getAdapter(adapterId);
				if (adapter == null) {
					LOGGER.error("DataAdapter does not exist");
					return null;
				}
			}
			if (!adapterMatchVerified) {
				if (!adapterId.equals(adapter.getAdapterId())) {
					return null;
				}
				adapterMatchVerified = true;
			}
			final ByteArrayId fieldId = new ByteArrayId(
					entry.getKey().getColumnQualifierData().getBackingArray());
			final CommonIndexModel indexModel = index.getIndexModel();
			// first check if this field is part of the index model
			final FieldReader<? extends CommonIndexValue> indexFieldReader = indexModel.getReader(fieldId);
			final byte byteValue[] = entry.getValue().get();
			if (indexFieldReader != null) {
				final CommonIndexValue indexValue = indexFieldReader.readField(byteValue);
				indexValue.setVisibility(entry.getKey().getColumnVisibilityData().getBackingArray());
				final PersistentValue<CommonIndexValue> val = new PersistentValue<CommonIndexValue>(
						fieldId,
						indexValue);
				indexData.addValue(val);
				fieldInfoList.add(DataStoreUtils.getFieldInfo(
						val,
						byteValue,
						indexValue.getVisibility()));
			}
			else {
				// next check if this field is part of the adapter's
				// extended data model
				final FieldReader<?> extFieldReader = adapter.getReader(fieldId);
				if (extFieldReader == null) {
					// if it still isn't resolved, log an error, and
					// continue
					LOGGER.error("field reader not found for data entry, the value may be ignored");
					unknownData.addValue(new PersistentValue<byte[]>(
							fieldId,
							byteValue));
					continue;
				}
				final Object value = extFieldReader.readField(byteValue);
				final PersistentValue<Object> val = new PersistentValue<Object>(
						fieldId,
						value);
				extendedData.addValue(val);
				fieldInfoList.add(DataStoreUtils.getFieldInfo(
						val,
						byteValue,
						entry.getKey().getColumnVisibility().getBytes()));
			}
		}
		final IndexedAdapterPersistenceEncoding encodedRow = new IndexedAdapterPersistenceEncoding(
				adapterId,
				new ByteArrayId(
						rowId.getDataId()),
				new ByteArrayId(
						rowId.getInsertionId()),
				rowId.getNumberOfDuplicates(),
				indexData,
				unknownData,
				extendedData);
		if ((clientFilter == null) || clientFilter.accept(
				index.getIndexModel(),
				encodedRow)) {
			// cannot get here unless adapter is found (not null)
			if (adapter == null) {
				LOGGER.error("Error, adapter was null when it should not be");
			}
			else {
				final Pair<T, DataStoreEntryInfo> pair = Pair.of(
						adapter.decode(
								encodedRow,
								index),
						new DataStoreEntryInfo(
								Arrays.asList(new ByteArrayId(
										k.getRowData().getBackingArray())),
								fieldInfoList));
				if (scanCallback != null) {
					scanCallback.entryScanned(
							pair.getRight(),
							pair.getLeft());
				}
				return pair;
			}
		}
		return null;
	}

	public static <T> DataStoreEntryInfo write(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final T entry,
			final Writer writer,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final DataStoreEntryInfo ingestInfo = DataStoreUtils.getIngestInfo(
				writableAdapter,
				index,
				entry,
				customFieldVisibilityWriter);
		final List<Mutation> mutations = buildMutations(
				writableAdapter.getAdapterId().getBytes(),
				ingestInfo);

		writer.write(mutations);
		return ingestInfo;
	}

	public static <T> void removeFromAltIndex(
			final WritableDataAdapter<T> writableAdapter,
			final List<ByteArrayId> rowIds,
			final T entry,
			final Writer writer ) {

		final byte[] adapterId = writableAdapter.getAdapterId().getBytes();
		final byte[] dataId = writableAdapter.getDataId(
				entry).getBytes();

		final List<Mutation> mutations = new ArrayList<Mutation>();

		for (final ByteArrayId rowId : rowIds) {

			final Mutation mutation = new Mutation(
					new Text(
							dataId));
			mutation.putDelete(
					new Text(
							adapterId),
					new Text(
							rowId.getBytes()));

			mutations.add(mutation);
		}
		writer.write(mutations);
	}

	public static <T> void writeAltIndex(
			final WritableDataAdapter<T> writableAdapter,
			final DataStoreEntryInfo entryInfo,
			final T entry,
			final Writer writer ) {

		final byte[] adapterId = writableAdapter.getAdapterId().getBytes();
		final byte[] dataId = writableAdapter.getDataId(
				entry).getBytes();
		if ((dataId != null) && (dataId.length > 0)) {
			final List<Mutation> mutations = new ArrayList<Mutation>();

			for (final ByteArrayId rowId : entryInfo.getRowIds()) {

				final Mutation mutation = new Mutation(
						new Text(
								dataId));
				mutation.put(
						new Text(
								adapterId),
						new Text(
								rowId.getBytes()),
						new Value(
								"".getBytes(StringUtils.UTF8_CHAR_SET)));

				mutations.add(mutation);
			}
			writer.write(mutations);
		}
	}

	public static <T> List<Mutation> entryToMutations(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final DataStoreEntryInfo ingestInfo = DataStoreUtils.getIngestInfo(
				dataWriter,
				index,
				entry,
				customFieldVisibilityWriter);
		return buildMutations(
				dataWriter.getAdapterId().getBytes(),
				ingestInfo);
	}

	private static <T> List<Mutation> buildMutations(
			final byte[] adapterId,
			final DataStoreEntryInfo ingestInfo ) {
		final List<Mutation> mutations = new ArrayList<Mutation>();
		final List<FieldInfo> fieldInfoList = ingestInfo.getFieldInfo();
		for (final ByteArrayId rowId : ingestInfo.getRowIds()) {
			final Mutation mutation = new Mutation(
					new Text(
							rowId.getBytes()));
			for (final FieldInfo fieldInfo : fieldInfoList) {
				mutation.put(
						new Text(
								adapterId),
						new Text(
								fieldInfo.getDataValue().getId().getBytes()),
						new ColumnVisibility(
								fieldInfo.getVisibility()),
						new Value(
								fieldInfo.getWrittenValue()));
			}

			mutations.add(mutation);
		}
		return mutations;
	}

	/**
	 * 
	 * @param dataWriter
	 * @param index
	 * @param entry
	 * @return List of zero or more matches
	 */
	public static <T> List<ByteArrayId> getRowIds(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final T entry ) {
		final CommonIndexModel indexModel = index.getIndexModel();
		final AdapterPersistenceEncoding encodedData = dataWriter.encode(
				entry,
				indexModel);
		final List<ByteArrayId> insertionIds = encodedData.getInsertionIds(index);
		final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>(
				insertionIds.size());

		DataStoreUtils.addToRowIds(
				rowIds,
				insertionIds,
				dataWriter.getDataId(
						entry).getBytes(),
				dataWriter.getAdapterId().getBytes(),
				encodedData.isDeduplicationEnabled());

		return rowIds;
	}

	/**
	 * Get Namespaces
	 * 
	 * @param connector
	 */
	public static List<String> getNamespaces(
			final Connector connector ) {
		final List<String> namespaces = new ArrayList<String>();

		for (final String table : connector.tableOperations().list()) {
			final int idx = table.indexOf(AbstractAccumuloPersistence.METADATA_TABLE) - 1;
			if (idx > 0) {
				namespaces.add(table.substring(
						0,
						idx));
			}
		}
		return namespaces;
	}

	/**
	 * Get list of data adapters associated with the given namespace
	 * 
	 * @param connector
	 * @param namespace
	 */
	public static List<DataAdapter<?>> getDataAdapters(
			final Connector connector,
			final String namespace ) {
		final List<DataAdapter<?>> adapters = new ArrayList<DataAdapter<?>>();

		final AdapterStore adapterStore = new AccumuloAdapterStore(
				new BasicAccumuloOperations(
						connector,
						namespace));

		final Iterator<DataAdapter<?>> itr = adapterStore.getAdapters();

		while (itr.hasNext()) {
			adapters.add(itr.next());
		}

		return adapters;
	}

	/**
	 * Get list of indices associated with the given namespace
	 * 
	 * @param connector
	 * @param namespace
	 */
	public static List<Index> getIndices(
			final Connector connector,
			final String namespace ) {
		final List<Index> indices = new ArrayList<Index>();

		final IndexStore indexStore = new AccumuloIndexStore(
				new BasicAccumuloOperations(
						connector,
						namespace));

		final Iterator<Index> itr = indexStore.getIndices();

		while (itr.hasNext()) {
			indices.add(itr.next());
		}

		return indices;
	}

	/**
	 * Set splits on a table based on quantile distribution and fixed number of
	 * splits
	 * 
	 * @param namespace
	 * @param index
	 * @param quantile
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setSplitsByQuantile(
			final Connector connector,
			final String namespace,
			final Index index,
			final int quantile )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final SortedSet<Text> splits = new TreeSet<Text>();

		final CloseableIterator<Entry<Key, Value>> iterator = getIterator(
				connector,
				namespace,
				index);

		if (iterator == null) {
			LOGGER.error("could not get iterator instance, getIterator returned null");
			throw new IOException(
					"could not get iterator instance, getIterator returned null");
		}

		final int numberSplits = quantile - 1;
		BigInteger min = null;
		BigInteger max = null;

		while (iterator.hasNext()) {
			final Entry<Key, Value> entry = iterator.next();
			final byte[] bytes = entry.getKey().getRow().getBytes();
			final BigInteger value = new BigInteger(
					bytes);
			if ((min == null) || (max == null)) {
				min = value;
				max = value;
			}
			min = min.min(value);
			max = max.max(value);
		}

		final BigDecimal dMax = new BigDecimal(
				max);
		final BigDecimal dMin = new BigDecimal(
				min);
		BigDecimal delta = dMax.subtract(dMin);
		delta = delta.divideToIntegralValue(new BigDecimal(
				quantile));

		for (int ii = 1; ii <= numberSplits; ii++) {
			final BigDecimal temp = delta.multiply(BigDecimal.valueOf(ii));
			final BigInteger value = min.add(temp.toBigInteger());

			final Text split = new Text(
					value.toByteArray());
			splits.add(split);
		}

		final String tableName = AccumuloUtils.getQualifiedTableName(
				namespace,
				StringUtils.stringFromBinary(index.getId().getBytes()));
		connector.tableOperations().addSplits(
				tableName,
				splits);
		connector.tableOperations().compact(
				tableName,
				null,
				null,
				true,
				true);
	}

	/**
	 * Set splits on table based on equal interval distribution and fixed number
	 * of splits.
	 * 
	 * @param namespace
	 * @param index
	 * @param numberSplits
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setSplitsByNumSplits(
			final Connector connector,
			final String namespace,
			final Index index,
			final int numberSplits )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final long count = getEntries(
				connector,
				namespace,
				index);

		final CloseableIterator<Entry<Key, Value>> iterator = getIterator(
				connector,
				namespace,
				index);

		if (iterator == null) {
			LOGGER.error("Could not get iterator instance, getIterator returned null");
			throw new IOException(
					"Could not get iterator instance, getIterator returned null");
		}

		long ii = 0;
		final long splitInterval = count / numberSplits;
		final SortedSet<Text> splits = new TreeSet<Text>();
		while (iterator.hasNext()) {
			final Entry<Key, Value> entry = iterator.next();
			ii++;
			if (ii >= splitInterval) {
				ii = 0;
				splits.add(entry.getKey().getRow());
			}
		}

		final String tableName = AccumuloUtils.getQualifiedTableName(
				namespace,
				StringUtils.stringFromBinary(index.getId().getBytes()));
		connector.tableOperations().addSplits(
				tableName,
				splits);
		connector.tableOperations().compact(
				tableName,
				null,
				null,
				true,
				true);
	}

	/**
	 * Set splits on table based on fixed number of rows per split.
	 * 
	 * @param namespace
	 * @param index
	 * @param numberRows
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setSplitsByNumRows(
			final Connector connector,
			final String namespace,
			final Index index,
			final long numberRows )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final CloseableIterator<Entry<Key, Value>> iterator = getIterator(
				connector,
				namespace,
				index);

		if (iterator == null) {
			LOGGER.error("Unable to get iterator instance, getIterator returned null");
			throw new IOException(
					"Unable to get iterator instance, getIterator returned null");
		}

		long ii = 0;
		final SortedSet<Text> splits = new TreeSet<Text>();
		while (iterator.hasNext()) {
			final Entry<Key, Value> entry = iterator.next();
			ii++;
			if (ii >= numberRows) {
				ii = 0;
				splits.add(entry.getKey().getRow());
			}
		}

		final String tableName = AccumuloUtils.getQualifiedTableName(
				namespace,
				StringUtils.stringFromBinary(index.getId().getBytes()));
		connector.tableOperations().addSplits(
				tableName,
				splits);
		connector.tableOperations().compact(
				tableName,
				null,
				null,
				true,
				true);
	}

	/**
	 * Check if locality group is set.
	 * 
	 * @param namespace
	 * @param index
	 * @param adapter
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static boolean isLocalityGroupSet(
			final Connector connector,
			final String namespace,
			final Index index,
			final DataAdapter<?> adapter )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		// get unqualified table name
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		return operations.localityGroupExists(
				tableName,
				adapter.getAdapterId().getBytes());
	}

	/**
	 * Set locality group.
	 * 
	 * @param namespace
	 * @param index
	 * @param adapter
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws TableNotFoundException
	 */
	public static void setLocalityGroup(
			final Connector connector,
			final String namespace,
			final Index index,
			final DataAdapter<?> adapter )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		// get unqualified table name
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		operations.addLocalityGroup(
				tableName,
				adapter.getAdapterId().getBytes());
	}

	/**
	 * Get number of entries for a data adapter in an index.
	 * 
	 * @param namespace
	 * @param index
	 * @param adapter
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	public static long getEntries(
			final Connector connector,
			final String namespace,
			final Index index,
			final DataAdapter<?> adapter )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		long counter = 0L;
		final AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);
		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				operations);
		if (indexStore.indexExists(index.getId()) && adapterStore.adapterExists(adapter.getAdapterId())) {
			final List<ByteArrayId> adapterIds = new ArrayList<ByteArrayId>();
			adapterIds.add(adapter.getAdapterId());
			final AccumuloConstraintsQuery accumuloQuery = new AccumuloConstraintsQuery(
					adapterIds,
					index);
			final CloseableIterator<?> iterator = accumuloQuery.query(
					operations,
					new AccumuloAdapterStore(
							operations),
					null);
			while (iterator.hasNext()) {
				counter++;
				iterator.next();
			}
			iterator.close();
		}
		return counter;
	}

	/**
	 * Get number of entries per index.
	 * 
	 * @param namespace
	 * @param index
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 */
	public static long getEntries(
			final Connector connector,
			final String namespace,
			final Index index )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		long counter = 0L;
		final AccumuloOperations operations = new BasicAccumuloOperations(
				connector,
				namespace);
		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);
		if (indexStore.indexExists(index.getId())) {
			final AccumuloConstraintsQuery accumuloQuery = new AccumuloConstraintsQuery(
					index);
			final CloseableIterator<?> iterator = accumuloQuery.query(
					operations,
					new AccumuloAdapterStore(
							operations),
					null);
			while (iterator.hasNext()) {
				counter++;
				iterator.next();
			}
			iterator.close();
		}
		return counter;
	}

	public static void attachRowMergingIterators(
			final RowMergingDataAdapter<?, ?> adapter,
			final AccumuloOperations operations,
			final String tableName,
			final boolean createTable )
			throws TableNotFoundException {
		final EnumSet<IteratorScope> visibilityCombinerScope = EnumSet.of(IteratorScope.scan);
		final OptionProvider optionProvider = new RowMergingAdapterOptionProvider(
				adapter);
		final RowTransform rowTransform = adapter.getTransform();
		final IteratorConfig rowMergingCombinerConfig = new IteratorConfig(
				EnumSet.complementOf(visibilityCombinerScope),
				rowTransform.getBaseTransformPriority(),
				rowTransform.getTransformName() + ROW_MERGING_SUFFIX,
				RowMergingCombiner.class.getName(),
				optionProvider);
		final IteratorConfig rowMergingVisibilityCombinerConfig = new IteratorConfig(
				visibilityCombinerScope,
				rowTransform.getBaseTransformPriority() + 1,
				rowTransform.getTransformName() + ROW_MERGING_VISIBILITY_SUFFIX,
				RowMergingVisibilityCombiner.class.getName(),
				optionProvider);

		operations.attachIterators(
				tableName,
				createTable,
				rowMergingCombinerConfig,
				rowMergingVisibilityCombinerConfig);
	}

	private static CloseableIterator<Entry<Key, Value>> getIterator(
			final Connector connector,
			final String namespace,
			final Index index )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		CloseableIterator<Entry<Key, Value>> iterator = null;
		final AccumuloOperations operations = new BasicAccumuloOperations(
				connector);
		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);
		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				operations);

		if (indexStore.indexExists(index.getId())) {
			final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
			final ScannerBase scanner = operations.createBatchScanner(tableName);
			((BatchScanner) scanner).setRanges(AccumuloUtils.byteArrayRangesToAccumuloRanges(null));

			final IteratorSetting iteratorSettings = new IteratorSetting(
					10,
					"GEOWAVE_WHOLE_ROW_ITERATOR",
					WholeRowIterator.class);
			scanner.addScanIterator(iteratorSettings);

			final List<QueryFilter> clientFilters = new ArrayList<QueryFilter>();
			clientFilters.add(
					0,
					new DedupeFilter());

			final Iterator<Entry<Key, Value>> it = new IteratorWrapper(
					adapterStore,
					index,
					scanner.iterator(),
					new FilterList<QueryFilter>(
							clientFilters));

			iterator = new CloseableIteratorWrapper<Entry<Key, Value>>(
					new ScannerClosableWrapper(
							scanner),
					it);
		}
		return iterator;
	}

	private static class IteratorWrapper implements
			Iterator<Entry<Key, Value>>
	{

		private final Iterator<Entry<Key, Value>> scannerIt;
		private final AdapterStore adapterStore;
		private final Index index;
		private final QueryFilter clientFilter;
		private Entry<Key, Value> nextValue;

		public IteratorWrapper(
				final AdapterStore adapterStore,
				final Index index,
				final Iterator<Entry<Key, Value>> scannerIt,
				final QueryFilter clientFilter ) {
			this.adapterStore = adapterStore;
			this.index = index;
			this.scannerIt = scannerIt;
			this.clientFilter = clientFilter;
			findNext();
		}

		private void findNext() {
			while (scannerIt.hasNext()) {
				final Entry<Key, Value> row = scannerIt.next();
				final Object decodedValue = decodeRow(
						row,
						clientFilter,
						index);
				if (decodedValue != null) {
					nextValue = row;
					return;
				}
			}
			nextValue = null;
		}

		private Object decodeRow(
				final Entry<Key, Value> row,
				final QueryFilter clientFilter,
				final Index index ) {
			return AccumuloUtils.decodeRow(
					row.getKey(),
					row.getValue(),
					new AccumuloRowId(
							row.getKey()), // need to pass this, otherwise null
											// value for rowId gets dereferenced
											// later
					adapterStore,
					clientFilter,
					index);
		}

		@Override
		public boolean hasNext() {
			return nextValue != null;
		}

		@Override
		public Entry<Key, Value> next() {
			final Entry<Key, Value> previousNext = nextValue;
			findNext();
			return previousNext;
		}

		@Override
		public void remove() {}
	}

	public static void handleSubsetOfFieldIds(
			final ScannerBase scanner,
			final Index index,
			final Collection<String> fieldIds,
			final CloseableIterator<DataAdapter<?>> dataAdapters ) {

		Set<ByteArrayId> uniqueDimensions = new HashSet<>();
		for (final DimensionField<? extends CommonIndexValue> dimension : index.getIndexModel().getDimensions()) {
			uniqueDimensions.add(dimension.getFieldId());
		}

		while (dataAdapters.hasNext()) {

			final Text colFam = new Text(
					dataAdapters.next().getAdapterId().getBytes());

			// dimension fields must be included
			for (ByteArrayId dimension : uniqueDimensions) {
				scanner.fetchColumn(
						colFam,
						new Text(
								dimension.getBytes()));
			}

			// configure scanner to fetch only the specified fieldIds
			for (String fieldId : fieldIds) {
				scanner.fetchColumn(
						colFam,
						new Text(
								StringUtils.stringToBinary(fieldId)));
			}
		}

		try {
			dataAdapters.close();
		}
		catch (IOException e) {
			LOGGER.error(
					"Unable to close iterator",
					e);
		}

	}
}

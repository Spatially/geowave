package mil.nga.giat.geowave.datastore.accumulo.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.Query;

import org.apache.accumulo.core.client.ScannerBase;

/**
 * Represents a query operation for a specific set of Accumulo row IDs.
 * 
 */
public class AccumuloRowIdsQuery<T> extends
		AccumuloConstraintsQuery
{
	final Collection<ByteArrayId> rows;

	public AccumuloRowIdsQuery(
			final DataAdapter<T> adapter,
			final Index index,
			final Collection<ByteArrayId> rows,
			final ScanCallback<T> scanCallback,
			final DedupeFilter dedupFilter,
			final Collection<String> fieldIds,
			final String[] authorizations ) {
		super(
				Collections.singletonList(adapter.getAdapterId()),
				index,
				(Query) null,
				(DedupeFilter) dedupFilter,
				scanCallback,
				fieldIds,
				authorizations);
		this.rows = rows;
	}

	public AccumuloRowIdsQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final Collection<ByteArrayId> rows,
			final ScanCallback<T> scanCallback,
			final DedupeFilter dedupFilter,
			final Collection<String> fieldIds,
			final String[] authorizations ) {
		super(
				adapterIds,
				index,
				(Query) null,
				(DedupeFilter) dedupFilter,
				scanCallback,
				fieldIds,
				authorizations);
		this.rows = rows;
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		for (ByteArrayId row : rows)
			ranges.add(new ByteArrayRange(
					row,
					row));
		return ranges;
	}
}
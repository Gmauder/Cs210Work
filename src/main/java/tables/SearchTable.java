package tables;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import sql.FieldType;

/**
 * Implements a search-based table
 * using a list data structure.
 */
public class SearchTable extends Table {
	
	//Map : { key: value, key: value, key: value}
	//Map : { "A": 1, "B": 2, "C": 3}
	private Map<Object, List<Object>> tree;
	private int fingerprint;

	/**
	 * Creates a table and initializes
	 * the data structure.
	 *
	 * @param tableName the table name
	 * @param columnNames the column names
	 * @param columnTypes the column types
	 * @param primaryIndex the primary index
	 */
	public SearchTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);

		tree = new TreeMap<>();
		clear();
	}

	@Override
	public void clear() {
		tree.clear();
		fingerprint = 0;
	}

	@Override
	public boolean put(List<Object> row) {
		row = sanitizeRow(row);

		Object key = row.get(getPrimaryIndex());
		List<Object> old = tree.put(key, row); // local variable type inference
		if(old != null) {
			fingerprint += row.hashCode() - old.hashCode();
			return true;
		}
		fingerprint += row.hashCode();
		return false;
		

	}

	@Override
	public boolean remove(Object key) {
		List<Object>row =tree.remove(key);
		if(row != null) {
			fingerprint -= row.hashCode();
			return true;
		}
		return false;
		
		
	}

	@Override
	public List<Object> get(Object key) {
		
		return tree.get(key);
		/*for (List<Object> row: list) {
			if (row.get(getPrimaryIndex()).equals(key))
				return row;
		}

		return null;
		*/
	}

	@Override
	public int size() {
		return tree.size();
	}

	@Override
	public int capacity() {
		return size();
	}

	@Override
	public int hashCode() {
		return fingerprint;
	}

	@Override
	public Iterator<List<Object>> iterator() {
		return tree.values().iterator();
		};
	}

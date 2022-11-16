package drivers;

import static sql.FieldType.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.FieldType;
import sql.QueryError;
import tables.HashArrayTable;
import tables.SearchTable;
import tables.Table;

/*
 * SHOW TABLE example_table
 * 	 -> table: example_table from database
 */
public class DropColumn implements Driver {
	private static final Pattern pattern = Pattern.compile(
			//ALTER\s*TABLE\s*([a-z][a-z0-9_]*)\s*DROP\s*COLUMN\s*([a-z][a-z0-9_]*)
		"ALTER\\s*TABLE\\s*([a-z][a-z0-9_]*)\\s*DROP\\s*COLUMN\\s*([a-z][a-z0-9_]*)",
		Pattern.CASE_INSENSITIVE
	);

	private String tableName;
	private String colName;
	

	@Override
	public boolean parse(String query) {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		tableName = matcher.group(1);
		colName = matcher.group(2);
	

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		Table table = db.find(tableName);
		List<String> columnNames = table.getColumnNames();
		List<FieldType> columnTypes = table.getColumnTypes();
		int prime = table.getPrimaryIndex();
		
		List<String>colNames = new ArrayList<String>();
		List<FieldType>colTypes = new ArrayList<FieldType>();
		
		
		var rows = new ArrayList<>(table.rows());
		rows.sort(new Comparator<List<Object>>() {

			@SuppressWarnings("rawtypes")
			@Override
			public int compare(List<Object> row1, List<Object> row2) {
				
				var key1 = (Comparable)row1.get(0); // use p.i
				var key2 = (Comparable)row2.get(0); //use p.i
				return key1.compareTo(key2);
				// TODO Auto-generated method stub
				//return 0;
			}
		});
		
		
		for(String name: columnNames) {
			colNames.add(name);
		}
		for(var type: columnTypes) {
			colTypes.add(type);
		}
		
			if (table == null)
			throw new QueryError("Missing table <%s>".formatted(tableName));
		
			
		if(columnNames.contains(colName)) {
			if(columnNames.indexOf(colName) < prime)
			{
				prime--;
			}
			colTypes.remove(columnNames.indexOf(colName));
			colNames.remove(columnNames.indexOf(colName));
			for(var row: rows) {
				row.remove(columnNames.indexOf(colName));
			}
		}
		
		
		
		Table table1 = new HashArrayTable( //TODO: update schema based on the requirements
				tableName,
				colNames,
				colTypes,
				prime
			);
		
		
	
		db.drop(tableName);
		db.create(table1);
		table1.putAll(rows);

		return table1;
	}
}

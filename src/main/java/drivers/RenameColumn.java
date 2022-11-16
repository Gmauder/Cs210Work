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
public class RenameColumn implements Driver {
	private static final Pattern pattern = Pattern.compile(
			//ALTER\s*TABLE\s*([a-z][a-z0-9_]*)\s*RENAME\s*COLUMN\s*([a-z][a-z0-9_]*)\s*TO\s*([a-z][a-z0-9_]*)
		"ALTER\\s*TABLE\\s*([a-z][a-z0-9_]*)\\s*RENAME\\s*COLUMN\\s*([a-z][a-z0-9_]*)\\s*TO\\s*([a-z][a-z0-9_]*)",
		Pattern.CASE_INSENSITIVE
	);

	private String tableName;
	private String oldColName;
	private String newColName;
	

	@Override
	public boolean parse(String query) {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		tableName = matcher.group(1);
		oldColName = matcher.group(2);
		newColName = matcher.group(3);

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
		
		
		colNames.set(colNames.indexOf(oldColName), newColName);
		
		
		
		Table table1 = new HashArrayTable( //TODO: update schema based on the requirements
				tableName,
				colNames,
				columnTypes,
				prime
			);
		
		
	
		db.drop(tableName);
		db.create(table1);
		table1.putAll(rows);

		return table1;
	}
}

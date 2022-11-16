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
public class AddColumn implements Driver {
	private static final Pattern pattern = Pattern.compile(
			//ALTER\s*TABLE\s*([a-z][a-z0-9_]*)\s*ADD\s*COLUMN\s*((?:(?:[a-z]|\d)+) (?:(?:STRING)|(?:INTEGER)|(?:BOOLEAN))?)\s*((?:(?:FIRST)|(?:BEFORE|AFTER)\s*(?:[a-z][a-z0-9_]*)|(?:LAST)))
		"ALTER\\s*TABLE\\s*([a-z][a-z0-9_]*)\\s*ADD\\s*COLUMN\\s*((?:(?:[a-z]|\\d)+) (?:(?:STRING)|(?:INTEGER)|(?:BOOLEAN))?)\\s*((?:(?:FIRST)|(?:BEFORE|AFTER)\\s*(?:[a-z][a-z0-9_]*)|(?:LAST)))",
		Pattern.CASE_INSENSITIVE
	);

	private String tableName;
	private String colDef;
	private String place;

	@Override
	public boolean parse(String query) {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		tableName = matcher.group(1);
		colDef = matcher.group(2);
		place = matcher.group(3);

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
		
		String elems[] = colDef.split(" ");
		String w[] = place.split(" ");
		if(w[0].equalsIgnoreCase("FIRST")){
			
			colNames.add(0, elems[0]);
			prime++;
			
			if(elems[1].toUpperCase().equals("INTEGER")) {
				colTypes.add(0, INTEGER);
				for(var row : rows) {
					row.add(0, 0);
				}
			}
			
			if(elems[1].toUpperCase().equals("BOOLEAN")) {
				colTypes.add(0, BOOLEAN);
				for(var row : rows) {
					row.add(0, true);
				}
			}
			
			if(elems[1].toUpperCase().equals("STRING")) {
				colTypes.add(0, STRING);
				for(var row : rows) {
					row.add(0, " ");
				}
			}
		}
		else if (w[0].equalsIgnoreCase("LAST")){
			colNames.add(elems[0]);
			
			for(var row : rows) {
				row.add(null);
			}
			
			if(elems[1].toUpperCase().equals("INTEGER")) {
				colTypes.add(INTEGER);
			}
			
			if(elems[1].toUpperCase().equals("BOOLEAN")) {
				colTypes.add(BOOLEAN);
			}
			
			if(elems[1].toUpperCase().equals("STRING")) {
				colTypes.add(STRING);
			}
		}
		else if (w[0].equalsIgnoreCase("BEFORE")){
			colNames.add(colNames.indexOf(w[1]) - 1, elems[0]);
			
			for(var row : rows) {
				row.add(colNames.indexOf(w[1]) - 1, null);
			}
			
			if((colNames.indexOf(w[1]) - 1) < prime) {
				prime++;
			}
			
			if(elems[1].toUpperCase().equals("INTEGER")) {
				colTypes.add(colNames.indexOf(w[1]) - 1, INTEGER);
			}
			
			if(elems[1].toUpperCase().equals("BOOLEAN")) {
				colTypes.add(colNames.indexOf(w[1]) - 1, BOOLEAN);
			}
			
			if(elems[1].toUpperCase().equals("STRING")) {
				colTypes.add(colNames.indexOf(w[1]) - 1, STRING);
			}
		}
		else {
			colNames.add(colNames.indexOf(w[1]) + 1, elems[0]);
			
			for(var row : rows) {
				row.add(colNames.indexOf(w[1]) + 1, null);
			}
			
			
			if((colNames.indexOf(w[1]) + 1) < prime) {
				prime++;
			}
			
			if(elems[1].toUpperCase().equals("INTEGER")) {
				colTypes.add(colNames.indexOf(w[1]) + 1, INTEGER);
			}
			
			if(elems[1].toUpperCase().equals("BOOLEAN")) {
				colTypes.add(colNames.indexOf(w[1]) + 1, BOOLEAN);
			}
			
			if(elems[1].toUpperCase().equals("STRING")) {
				colTypes.add(colNames.indexOf(w[1]) + 1, STRING);
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

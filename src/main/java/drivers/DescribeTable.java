package drivers;

import static sql.FieldType.*;
import static sql.FieldType.STRING;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.QueryError;
import tables.HashArrayTable;
import tables.SearchTable;
import tables.Table;

/*
 * SHOW TABLE example_table
 * 	 -> table: example_table from database
 */
public class DescribeTable implements Driver {
	private static final Pattern pattern = Pattern.compile(
		"DESCRIBE\\s+TABLE\\s+([a-z][a-z0-9_]*)",
		Pattern.CASE_INSENSITIVE
	);

	private String tableName;

	@Override
	public boolean parse(String query) {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		tableName = matcher.group(1);

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		Table table = db.find(tableName);
		
		Table resultSet = new HashArrayTable( //TODO: update schema based on the requirements
				"_columns",
				List.of("index", "name", "type", "is_primary"),
				List.of(INTEGER, STRING, STRING, BOOLEAN),
				0
			);

		if (table == null)
			throw new QueryError("Missing table <%s>".formatted(tableName));
		
		
		
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
		int counter = 0;
		for(List<Object> row : rows) {
		//counter++;
		row.add(0, counter);
		if(row.get(2) != null) {
		row.set(2, row.get(2).toString());
		}
		resultSet.put(row);
		counter++;
		}

		return resultSet;
	}
}

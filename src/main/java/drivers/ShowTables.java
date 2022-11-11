package drivers;

import static sql.FieldType.*;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.QueryError;
import tables.SearchTable;
import tables.Table;


public class ShowTables implements Driver {
	private static final Pattern pattern = Pattern.compile(
		//SHOW\s+TABLES
		"SHOW\\s+TABLES",
		Pattern.CASE_INSENSITIVE
	);

	

	@Override
	public boolean parse(String query) throws QueryError {
		Matcher matcher = pattern.matcher(query.strip());
		


		return matcher.matches();
		
		//TODO: convert lines 25 to 28 into a single return statement
	}

	@Override
	public Object execute(Database db) {
		Table resultSet = new SearchTable( //TODO: update schema based on the requirements
			"_tables",
			List.of("table_name", "column_count", "row_count"),
			List.of(STRING, INTEGER, INTEGER),
			0
		);

		//for each table in the databases list of tables:
		//(enhanced for loop)
		//for each table in the db's list of tables
		
	for(var table: db.tables()) {
			List<Object> row = new LinkedList<>();
			row.add(table.getTableName()); //name of table, get from table's schema
			row.add(table.getColumnTypes().size()); // # columns, count the table's columns in the shema
			row.add(table.rows().size()); // # rows, use the same technique as drop tables return
			resultSet.put(row);
			
			//resultSet.put(List.of(i));
		}

		return resultSet;
	}
}

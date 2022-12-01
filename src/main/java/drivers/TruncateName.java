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
public class TruncateName implements Driver {
	private static final Pattern pattern = Pattern.compile(
			//\s*TRUNCATE\s*TABLE\s*([a-z0-9_]+)
		"\\s*TRUNCATE\\s*TABLE\\s*([a-z0-9_]+)",
		Pattern.CASE_INSENSITIVE
	);

	private String TableName;
	
	

	@Override
	public boolean parse(String query) {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		TableName = matcher.group(1);
		

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		Table table = db.find(TableName);
		
		
		
		
		
		var rows = new ArrayList<>(table.rows());
		
		int numRows = 0;
		for(var row: rows) {
			numRows++;
		}
			
		
		table.clear();
		
		return numRows;
		
		
		
		
		
		
		
		
	
		
	}
}

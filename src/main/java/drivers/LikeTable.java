package drivers;

import static sql.FieldType.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.FieldType;
import sql.QueryError;
import tables.HashArrayTable;
//import tables.SearchTable;
import tables.Table;


public class LikeTable implements Driver {
	private static final Pattern pattern = Pattern.compile(
			//CREATE\s*TABLE\s*([a-z0-9_]+)\s*LIKE\s*([a-z0-9_]+)
		"CREATE\\s*TABLE\\s*([a-z0-9_]+)\\s*LIKE\\s*([a-z0-9_]+)",
		Pattern.CASE_INSENSITIVE
	);

	private String tableName;
	private String oldName;
	private List<String> columnNames;
	private List<FieldType> columnTypes;
	private int primaryIndex;

	
	@Override
	public boolean parse(String query) throws QueryError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;
		
		tableName = matcher.group(1);
		oldName = matcher.group(2);
		//System.out.println(tableName);
		//System.out.println(tableName.length());
		
		//TODO if the name is too long, throw an exception of type QueryError
		
		if(tableName.length() > 15)
			throw new QueryError("A table name must be 1 to 15 characters");
		
		
		
			
			
			return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		// TODO: similar to other drivers
		
		if(db.exists(tableName)) {
			throw new QueryError("there cannot be duplicate table names");
		}
		
	
		//if tableName already exists in the db:
			//throw an error
		
		Table table = db.find(oldName);
		List<String> columnNames = table.getColumnNames();
		List<FieldType> columnTypes = table.getColumnTypes();
		int prime = table.getPrimaryIndex();
		
		Table table1 = new HashArrayTable( //TODO: update schema based on the requirements
				tableName,
				columnNames,
				columnTypes,
				prime
			);
		
		db.create(table1);
		//construct the table with the 4 schema properties
			//if the db is persistent( and have working m2)
				//when creating table, use the HashFileTable type
			//otherwise:
				//when creating table, use the HashArrayTable type
			//if m1/m2 dont work:
				//use searchTable
		
		//tell the db to create/add that table
		
		//return the table itself
		return table1;
	}
}

package drivers;

import static sql.FieldType.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apps.Database;
import sql.Driver;
import sql.FieldType;
import sql.QueryError;
import tables.HashArrayTable;
//import tables.SearchTable;
import tables.Table;


public class CreateTable implements Driver {
	private static final Pattern pattern = Pattern.compile(
			//CREATE\s*TABLE\s*([a-z][a-z0-9_]*)\s*\(((?:\s*,?\s*(?:(?:[a-z]|\d)+) (?:(?:STRING)|(?:DECIMAL)|(?:DECIMAL\s*\(([0-9]+)\)?)|(?:INTEGER(?:\s*AUTO_INCREMENT)?)|(?:BOOLEAN))(?: PRIMARY)?)*)\s*\)
		"CREATE\\s*TABLE\\s*([a-z][a-z0-9_]*)\\s*\\(((?:\\s*,?\\s*(?:(?:[a-z]|\\d)+) (?:(?:STRING)|(?:DECIMAL)|(?:DECIMAL\\s*\\(([0-9]+)\\)?)|(?:INTEGER(?:\\s*AUTO_INCREMENT)?)|(?:BOOLEAN))(?: PRIMARY)?)*)\\s*\\)",
		Pattern.CASE_INSENSITIVE
	);

	private String tableName;
	private List<String> columnNames;
	private List<FieldType> columnTypes;
	private int primaryIndex;
	ArrayList<Boolean> autocols = new ArrayList<Boolean>();
	ArrayList<Integer> colScales = new ArrayList<Integer>();
	
	@Override
	public boolean parse(String query) throws QueryError {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;
		
		tableName = matcher.group(1);
		System.out.println(tableName);
		System.out.println(tableName.length());
		
		//TODO if the name is too long, throw an exception of type QueryError
		
		if(tableName.length() > 15)
			throw new QueryError("A table name must be 1 to 15 characters");
		
		// initialize columnNames to an empty list
		columnNames = new ArrayList<String>();
		// initialize columnTypes to an empty list
		columnTypes = new ArrayList<FieldType>();
		// initialize primaryIndex to be -1 (fake index)
		 primaryIndex = -1;
		//colDefs array is the result of splitting group 2 on commas
		String colDefs[] = matcher.group(2).split(",");
		
		if(matcher.group(2).equals("")) {
			throw new QueryError("There must be at least one column");
		}
		//for each colDef in colDefs array:
		for(int i = 0; i < colDefs.length; i++)
		{
			
			colDefs[i] = colDefs[i].strip();
			String ind[] = colDefs[i].split(" ");
			//ind[0].strip();
			//strip and split on whitespace
			// guaranteed to have either 2 or 3 words
			for(int a = 0; a < ind.length; a++) {
				
				//ind[a].strip();
				System.out.print(ind[a] + " ");
			}
			System.out.println();
			if((ind.length == 3 || ind.length == 4) && ind[ind.length-1].toUpperCase().equals("PRIMARY")) {
				if(primaryIndex == -1) {
					primaryIndex = i;
				}
				else {
					throw new QueryError("There can only be one primary index");
				}
			}
			
			if(ind[0].length() > 15) {
				throw new QueryError("max column name length is 15");
			}
			//System.out.println(ind[0]);
			System.out.println(columnNames.contains(ind[0]));
			if(columnNames.contains(ind[0])) {
				throw new QueryError("there cannot be duplicate column names");
			}
			//System.out.println(ind[0]);
			columnNames.add(ind[0]);
			
			if(ind[1].toUpperCase().equals("INTEGER")) {
				columnTypes.add(INTEGER);
				colScales.add(-1);
				if(ind.length > 2) {
				if(ind[2].toUpperCase().equals("AUTO_INCREMENT")) {
					autocols.add(true);
					
				}
			}
				else {
					autocols.add(false);
					
				}
				
			}
			
			if(ind[1].toUpperCase().equals("BOOLEAN")) {
				columnTypes.add(BOOLEAN);
				autocols.add(false);
				colScales.add(-1);
			}
			
			if(ind[1].toUpperCase().equals("STRING")) {
				columnTypes.add(STRING);
				autocols.add(false);
				colScales.add(-1);
			}
			
			if(ind[1].toUpperCase().contains("DECIMAL")) {
				//System.out.println("hi");
				columnTypes.add(DECIMAL);
				if(!Objects.equals(matcher.group(3), null)){
					//System.out.println(Integer.parseInt(matcher.group(3)));
					colScales.add(Integer.parseInt(matcher.group(3)));
				}
				else {
					colScales.add(-1);
				}
				autocols.add(false);
			}
			
			
			
			
			//if 3 words and the word at index 2 is PRIMARY:
				//if this is the first time (so p.i. is still -1):
					//update the p.i. to be the index of this column
				//if this is NOT the first time: throw a QueryError
			
			//if column name (0th elem of colDef) is too long:
				//throw and error
			
			//if the column name is already contained in the columnNames list:
				//throw an error
			
			// add the name onto the end of the columnNames list
			
			// add the type (1st elem of colDef) to the columnTypes list
				//HINT: check FieldType API
			
			//if columnNames list is too long:
			//throw an error
		}
		
		
		if(primaryIndex == -1) {
			throw new QueryError("There must be a primary column");
		}
		//if primaryIndex was never initialized(still -1):
			//throw an error
		if(columnNames.size() > 15) {
			throw new QueryError("There cannot be more than 15 columns");
		}
		

		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		// TODO: similar to other drivers
		
		if(db.exists(tableName)) {
			throw new QueryError("there cannot be duplicate table names");
		}
		for(var e: colScales) {
			System.out.print(e + " ");
		}
		//if tableName already exists in the db:
			//throw an error
		Table table1 = new HashArrayTable( //TODO: update schema based on the requirements
				tableName,
				columnNames,
				columnTypes,
				primaryIndex,
				autocols,
				colScales
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

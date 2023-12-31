package drivers;

import static sql.FieldType.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
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
public class DeleteRow implements Driver {
	private static final Pattern pattern = Pattern.compile(
			//DELETE\s+FROM\s+([a-z0-9_]+)(\s+WHERE\s*([a-z0-9_]+)\s*(<|<>|>|=|>=|<=)\s*([a-z0-9"]+))?
		"DELETE\\s+FROM\\s+([a-z0-9_]+)(\\s+WHERE\\s*([a-z0-9_]+)\\s*(<|<>|>|=|>=|<=)\\s*([a-z0-9\"]+))?",
		Pattern.CASE_INSENSITIVE
	);

	private String tableName;
	private String value;
	private String where;
	private String leftName;
	private String rightName;
	private String oper;
	
	private static final Pattern literalPattern = InsertInto.literalPattern;
	

	@Override
	public boolean parse(String query) {
		Matcher matcher = pattern.matcher(query.strip());
		if (!matcher.matches())
			return false;

		tableName = matcher.group(1);
		
		
		where = matcher.group(2);
		leftName = matcher.group(3);
		rightName = matcher.group(5);
		oper = matcher.group(4);
		return true;
	}

	@Override
	public Object execute(Database db) throws QueryError {
		Table resultSet;
		
		Table table = db.find(tableName);
		
		List<String> columnNames = List.copyOf(table.getColumnNames());
		List<FieldType> columnTypes = List.copyOf(table.getColumnTypes());
		int prime = table.getPrimaryIndex();
		
		resultSet = new HashArrayTable( //TODO: update schema based on the requirements
				tableName,
				columnNames,
				columnTypes,
				prime
			);
		
		db.drop(tableName);
		db.create(resultSet);
		
		List<String>colNames = new ArrayList<String>();
		List<FieldType>colTypes = new ArrayList<FieldType>();
		
		var rows = new ArrayList<>(table.rows());
		int numrows = rows.size();
		
		int lIndex = 0;
		FieldType lType = null;
		FieldType rType = null;
		Object rValue = null;
		
		ArrayList<Integer> pointerList = new ArrayList<Integer>();
		int counter = 0;
		for (var col : columnNames) {
			pointerList.add(counter);

			counter++;

		}
		
		if(!Objects.equals(where, null)) {
		Matcher litMatcher = literalPattern.matcher(rightName);
		
		
		if (!litMatcher.matches()) {
			throw new QueryError("no matches");
		}

		// if the literal doesnt match any of the available types:
		// throw a query error
		//
		if ((!Objects.equals(litMatcher.group(1), null))) {
			if (litMatcher.group(1).length() > 127) {
				throw new QueryError("The string is too long");
			}
			rValue = litMatcher.group(1);
			rType = STRING;
		}

		else if (!Objects.equals(litMatcher.group(2), null)) {
			rValue = new BigDecimal(litMatcher.group(2));
			rType = DECIMAL;

//			System.out.println(scales.size() + "j");
//			System.out.println(scales);
//			if (scales.get(ncount - 2) != -1) {
//				System.out.println("hi");
//				System.out.println(litMatcher.group(2));
//
//				result = new BigDecimal(litMatcher.group(2)).setScale(scales.get(ncount - 2));
//				System.out.println(result);
//			} else {
//				System.out.println(litMatcher.group(2));
//				result = new BigDecimal(litMatcher.group(2));
//			}
	}
		// if the literal value matches string AND the schema type is also string
		// if the string is too long throw a query error
		// just use your regex with a capture group that doesnt include quotation marks
		// to get the non-quoted chars
		// assign the string as the local variable for the resulting value
		else if (!Objects.isNull(litMatcher.group(3))) {

			try {
				rValue = Integer.parseInt(litMatcher.group(3));
				rType = INTEGER;

			} catch (Exception e) {
				throw new QueryError("Literal cannot be parsed as an int");
			}
			if (litMatcher.group(3).charAt(0) == '0' && litMatcher.group(3).length() > 1) {
				throw new QueryError("Integer can not start with 0");
			}

		}
		// else if the literal value matches integer AND the schema type is also integer
		// parse the literal as an integer (even if it has a sign) if it cant be parsed
		// throw a query error
		// assign the parsed integer as the local variable for the resulting value
		// (Integer Object)

		else if ((!Objects.equals(litMatcher.group(4), null))) {
			try {
				rValue = Boolean.parseBoolean(litMatcher.group(4));
				rType = BOOLEAN;

			} catch (Exception e) {
				throw new QueryError("Literal cannot be parsed as an boolean");
			}
		} else if (Objects.equals(litMatcher.group(5), "null")) {
			rValue = null;
		} else {
			throw new QueryError("This is not cool");
		}
		
		
		
		
			System.out.println("a");
			lIndex = columnNames.indexOf(leftName);
			for(var row: rows) {
				ArrayList<Object> temp = new ArrayList<Object>();
			Boolean rowFlag = false;
				for(Object elem: row) {
					
				
				Object lVal = elem;
				//System.out.println(elem);
				Object resultVal;
				int compnum = 0;
				if(columnTypes.get(row.indexOf(elem)) == rType) {
					if(rType.equals(STRING)) {
						compnum = lVal.toString().compareTo(rValue.toString());
						System.out.println("a7");
						temp.add(lVal.toString());
					}
					else if(rType.equals(INTEGER)) {
						System.out.println(elem);
						//compnum = Integer.compare((Integer)lVal, (Integer)rValue);
						compnum = (Integer)rValue;
						compnum = ((Integer)rValue).compareTo((Integer)Integer.parseInt((lVal.toString())));
						//resultVal = Integer.parseInt(value);
						temp.add(Integer.parseInt(lVal.toString()));
						//System.out.println(lVal + " " + rValue); 
						//System.out.println(compnum);
						//System.out.println();
					}
					else if(rType.equals(BOOLEAN)) {
						//compnum = Boolean.compare((Boolean)lVal, (Boolean)rValue);
						compnum = ((Boolean)lVal).compareTo((Boolean)rValue);
						temp.add(Boolean.parseBoolean(value));
						System.out.println((Boolean)lVal);
						System.out.println("lh = %b, rhs = %b, cmp = %d".formatted(lVal,rValue,compnum));
					}
				}
				else {
					System.out.println("hi");
					compnum = lVal.toString().compareTo(rValue.toString());
					if(columnTypes.get(row.indexOf(elem)) == INTEGER) {
						temp.add(Integer.parseInt(lVal.toString()));
					}
					else if(columnTypes.get(row.indexOf(elem)) == STRING) {
						temp.add(lVal.toString());
					}
					else {
						temp.add(Boolean.parseBoolean(value));
					}
				}
//				System.out.println(oper);
				if(oper.equals("=")) {
					if(compnum == 0) {
						rowFlag = true;
//						System.out.println("a");
					//System.out.println("lh = %b, rhs = %b, cmp = %d, flag = %b".formatted(lVal,rValue,compnum,rowFlag));
					}
				}
					else if(oper.equals("<>")) {
						if(compnum != 0) {
							rowFlag = true;
						}
					}
					else if(oper.equals("<")) {
						if(compnum < 0) {
							rowFlag = true;
						}
					}
					else if(oper.equals(">")) {
						if(compnum > 0) {
							rowFlag = true;
						}
					}
					else if(oper.equals("<=")) {
						if(compnum <= 0) {
							rowFlag = true;
						}
					}
					else if(oper.equals(">=")) {
						if(compnum >= 0) {
							rowFlag = true;
						}
					}
				
			}
				System.out.println(rowFlag);
				if(rowFlag == false) {
					System.out.println(temp);
					resultSet.put(temp);
					numrows--;
				}
				
	
}
		
		
	
	}
		return numrows;
}
}
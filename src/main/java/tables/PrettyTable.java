package tables;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import sql.FieldType;

/**
 * Defines the protocols for a table
 * with a pretty string representation.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
public abstract class PrettyTable extends Table {
	@Override
	public String toString() {
		int maxLength = 15;
		StringBuilder sb = new StringBuilder();
		
		var rows = new ArrayList<>(rows());
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
		
		int prime = getPrimaryIndex();
		
		
		
		int hoBorder = maxLength * getColumnNames().size() + (1 + getColumnNames().size());
		
		for(int z = 0; z < hoBorder; z++) {
			sb.append("-");
		}
		
		sb.append("\n");
		//TODO stringify header (table name and column names)
		int nameLength = getTableName().length();
		sb.append("| ");
		if(getColumnNames().size() == 3) {
			
		
		for(int m = 0; m < maxLength; m++) {
			sb.append(" ");
		}
		
		if(nameLength < maxLength) {
			for(int n = 0; n < (maxLength - nameLength) / 2; n ++) {
				sb.append(" ");
			}
		}
		
		sb.append(getTableName());
		
		
		
		
			for(int n = nameLength + (maxLength - nameLength) / 2; n < maxLength; n ++) {
				sb.append(" ");
			}
		
		
		for(int n = 0; n < maxLength; n++) {
			sb.append(" ");
		}
		//sb.append(" |");
	}
		else {
			
			if(nameLength < maxLength) {
				for(int n = 0; n < (maxLength - nameLength) / 2; n ++) {
					sb.append(" ");
				}
			}
			
			sb.append(getTableName());
			
			
			
			
				for(int n = nameLength + (maxLength - nameLength) / 2; n < maxLength - 2; n ++) {
					sb.append(" ");
				}
				
				for(int n = 0; n < (maxLength * (getColumnNames().size()  - 1)) + getColumnNames().size() - 1; n++) {
					sb.append(" ");
				}
				
				
		}
		sb.append(" |");
		sb.append("\n");
	
		 

		for(int z = 0; z < hoBorder; z++) {
			sb.append("-");
		}
		sb.append("\n");
		
		sb.append("| ");
		
	
		
		
		
		
		
		
		for(String colName: getColumnNames()) {
			
			if(colName.length() < maxLength) {
				for(int n = 0; n < (maxLength - colName.length()) / 2; n ++) {
					sb.append(" ");
				}
			}
			sb.append(colName);
			if(prime == getColumnNames().indexOf(colName)) {
				sb.append("*");
			}
			
			for(int n = colName.length() + (maxLength - colName.length()) / 2; n < maxLength - 1; n ++) {
				sb.append(" ");
			}
			if(prime == getColumnNames().indexOf(colName)) {
				sb.deleteCharAt(sb.length()-1);
				}
			if(!colName.equals(getColumnNames().get(getColumnNames().size() - 1)))
			sb.append(" |");
			
		}
		sb.append("|");
		sb.append("\n");
		
		for(int z = 0; z < hoBorder; z++) {
			sb.append("-");
		}
		sb.append("\n");
		
		
			
			for(List<Object> row: rows) {
				
				var tempList = new ArrayList(row);
				List<FieldType> colTypes = new ArrayList<FieldType>();
				sb.append("| ");
				for(int x = 0; x < tempList.size(); x++) {
					int colLength= maxLength;
					colTypes = getColumnTypes();
					if(colTypes.get(x) == FieldType.STRING || colTypes.get(x) == FieldType.BOOLEAN){
						
					
					if(tempList.get(x) != null) {
					if(tempList.get(x).toString().length() < maxLength) {
					//	for(int n = 0; n < (maxLength - tempList.get(x).toString().length()) / 2; n ++) {
						//	sb.append(" ");
						//}
						if(colTypes.get(x) == FieldType.STRING) {
							
						colLength = colLength - 2;
					sb.append("\"" + tempList.get(x) + "\"");
						}
						else
							sb.append(tempList.get(x));
					
					for(int n = tempList.get(x).toString().length(); n < colLength - 1; n ++) {
						sb.append(" ");
					}
					}
					else {
						//sb.append("  ");
						sb.append("\"");
						sb.append(tempList.get(x).toString().substring(0, maxLength - 7));
						sb.append("...");
						sb.append("  ");
						
					}
					
					if(x != tempList.size() - 1) {
						sb.append(" |");
					}
					}
						
					
				
					else {
						//for(int v = 0; v < (maxLength - 4) / 2; v++) {
						//sb.append(" ");
						//}
						
						sb.append("    ");
						
						
						
						for(int n = 4; n < maxLength - 1; n ++) {
								sb.append(" ");
							}
						if(x != tempList.size() - 1) {
							sb.append(" |");
						}
					}
					}
					else if(colTypes.get(x) == FieldType.INTEGER) {
						if(tempList.get(x) != null) {
							if(tempList.get(x).toString().length() < maxLength) {
								for(int n = 0; n < (maxLength - tempList.get(x).toString().length()) - 1; n ++) {
									sb.append(" ");
								}
							
							sb.append(tempList.get(x));
							
							//for(int n = tempList.get(x).toString().length(); n < maxLength - 1; n ++) {
								//sb.append(" ");
							//}
							}
							else {
								//sb.append("  ");
								sb.append(tempList.get(x).toString().substring(0, maxLength - 7));
								sb.append("...");
								sb.append("   ");
								
							}
							
							if(x != tempList.size() - 1) {
								sb.append(" |");
							}
							}
						
					
					else {
						for(int v = 4; v < maxLength-1; v++) {
						sb.append(" ");
						}
						
						sb.append("    ");
						
						
						
						//for(int n = (4 + ((maxLength - 4) / 2)); n < maxLength - 1; n ++) {
						//		sb.append(" ");
						//	}
						if(x != tempList.size() - 1) {
							sb.append(" |");
						}
					}
					}
					else if(colTypes.get(x) == FieldType.DECIMAL) {
						if(tempList.get(x) != null) {
							if(tempList.get(x).toString().length() < maxLength) {
								for(int n = 0; n < (maxLength - tempList.get(x).toString().length()) - 1; n ++) {
									sb.append(" ");
								}
							
							sb.append(tempList.get(x));
							
							//for(int n = tempList.get(x).toString().length(); n < maxLength - 1; n ++) {
								//sb.append(" ");
							//}
							}
							else {
								//sb.append("  ");
								sb.append(tempList.get(x).toString().substring(0, maxLength - 7));
								sb.append("...");
								sb.append("   ");
								
							}
							
							if(x != tempList.size() - 1) {
								sb.append(" |");
							}
							}
						
					
					else {
						for(int v = 4; v < maxLength-1; v++) {
						sb.append(" ");
						}
						
						sb.append("    ");
						
						
						
						//for(int n = (4 + ((maxLength - 4) / 2)); n < maxLength - 1; n ++) {
						//		sb.append(" ");
						//	}
						if(x != tempList.size() - 1) {
							sb.append(" |");
						}
					}
					}
					
				}
				sb.append("|");
				sb.append("\n");
				//sb.append(row).append("\n"); //TODO actually stringify the rows
			}
			
			for(int z = 0; z < hoBorder; z++) {
				sb.append("-");
			}
			
			return sb.toString();
		/*
	
		 * TODO: Implement pretty format for Module 3.
		 */
		//return super.toString();
	}
}

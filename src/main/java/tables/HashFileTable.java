package tables;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import static java.nio.file.StandardOpenOption.*;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import sql.FieldType;

/**
 * Implements a hash-based table
 * using a directory tree structure.
 */
public class HashFileTable extends Table {
	private Path root;
	private FileChannel schema, metadata;
	
	private int size;
	private int fingerprint;
	
	//a field to store the maximum record width
	
	// fields for metadata (size, fingerprint)
	
	//define constants for the limits of the data types
	//eg. magic numbers, max column name length etc
	
	/**
	 * Creates a table and initializes
	 * the file structure.
	 *
	 * @param tableName a table name
	 * @param columnNames the column names
	 * @param columnTypes the column types
	 * @param primaryIndex the primary index
	 */
	public HashFileTable(String tableName, List<String> columnNames, List<FieldType> columnTypes, int primaryIndex) {
		// follow example code to do the following:
		
		root = Paths.get(tableName);
		
		
			delete();
		
		// assign the root based on the table name
		
		// if there is already a root, recursively delete it (suggest delete helper method)
		// create the directories for the root, if needed
		// assign the file channels for both schema and metadata
		
		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);
		// writeSchema(); //based on the example
		//measure the maximum width of a record
		
		clear();
		//which calls writeMetadata();
		
	}

	/**
	 * Reopens a table from an
	 * existing file structure.
	 *
	 * @param tableName a table name
	 */
	public HashFileTable(String tableName) {
		// follow example code to do the following:
			// assign the root based on the table name
		//ensure that the schema and metadata file exist
		//assign the file channels for both schema and metadata
		
		setTableName(tableName);
		//readSchema(); based on the example
		
		//measure the record width
		
		//readMetadata(); //based on example
	}

	@Override
	public void clear() {
		truncate();

		//reset the metadata
		//writeMetaData();
		
	}
	
	private void truncate() {
		//resolve the state folder
		//if its not there yet, just quit this method
		//if there is a state folder, recursively delete the state folder using a walk (as in the example)
		
			if(Files.exists(root)) {
				try {
					Files.walk(root)
						.sorted(Comparator.reverseOrder())
						.map(path -> path.toFile())
						.forEach(file -> file.delete());
					
					Files.createDirectories(root);
					metadata = FileChannel.open(root.resolve("metadata"), CREATE_NEW, READ, WRITE);
					
					
				
					
					
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
	}
			
				
			
	}
	//optional for M2
	private void delete() {
		truncate();
		//delete schema file
		//delete metadata file
		//delete root folder
		
	}

	@Override
	public boolean put(List<Object> row) {
		
		row = sanitizeRow(row);

		Object key = row.get(getPrimaryIndex());
		var digest = digest(key);
		
		var old = readRecord(digest); //identify hit/miss
		
		writeRecord(digest, row);
		//update the metadata based on hit/miss
		
		return false; //based on hit/miss
	}

	@Override
	public boolean remove(Object key) {
		//get the digest for the key
		
		//find the existing row (if any) for that digest
		//if its a miss, terminate method
	
		
		//if we make it here its a hit:
		//	delete the corresponding file
		//	
		//update the metadata based on hit/miss
		return false;
	}

	@Override
	public List<Object> get(Object key) {
		var digest = digest(key);
		
		return readRecord(digest);
	}
	
	private void writeRecord(String digest, List<Object> row) {
		//fill in from example code
	}
	
	private List<Object> readRecord(String digest){
		//fill in from example code
		
		//return a row on a hit
		//or else a null on a miss
		return null;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public int capacity() {
		return size;
	}
	
	private String digest(Object key) {
		try {
			var sha1 = MessageDigest.getInstance("SHA-1");
			sha1.update("salt".getBytes(UTF_8));
			sha1.update(key.toString().getBytes(UTF_8));
			
			var digest = sha1.digest();
			var hex = HexFormat.of().withLowerCase();
			return hex.formatHex(digest);
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Digest error", e);
		}
		
	}
	// "12345678abcdef" -> path 12/345678abcdef

	private Path pathOf(String digest) {
		//get the 2 prefix chars
		//get the 38 suffix chars
		
		//return a path from the root
		//which resolves the prefix
		//and then resolves the suffix
		
		//return the resolved path
		return null;
	}
	
	// path 12/e45678abcdef -> "12345678abcdef"
	private String digestOf(Path path) {
		//get the folder name (asking the path object for its parents filename as a string)
		//get the file name (asking the path object for its filename as a string)
		//concatenate them together
		return null;
	}
	
	@Override
	public int hashCode() {
		return fingerprint;
	}

	@Override
	public Iterator<List<Object>> iterator() {
		try {
		var state = root.resolve("state");
		return Files.walk(state)
			.filter(path -> !Files.isDirectory(path))
			.map(path -> digestOf(path))
			.map(digest -> readRecord(digest))
			.iterator();
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		//see the recursive file delete example code and follow a similar process
		
		//resolve the state folder
		//walk the state folder:
			//filter so that we are only considering files(not directories)
			// associate each of the files with its path (using digestOf)
			//associate each of the digests with the result of reading its record
		//return null;
	}
}

package tables;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import static java.nio.file.StandardOpenOption.*;
import static sql.FieldType.STRING;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
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
	
	private int maxWidth;
	
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
			try {
			if (Files.notExists(root.resolve("schema")))
				throw new IOException("Missing schema file");
			schema = FileChannel.open(root.resolve("schema"), READ, WRITE);
			
			if (Files.notExists(root.resolve("metadata")))
				throw new IOException("Missing metadata file");
			metadata = FileChannel.open(root.resolve("metadata"), READ, WRITE);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		
		setTableName(tableName);
		setColumnNames(columnNames);
		setColumnTypes(columnTypes);
		setPrimaryIndex(primaryIndex);
		 writeSchema(); //based on the example
		//measure the maximum width of a record
		
		clear();
		//which calls 
		writeMetaData();
		
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
		try {
		root = Paths.get(tableName);
		//ensure that the schema and metadata file exist
		//assign the file channels for both schema and metadata
		
		setTableName(tableName);
		//readSchema(); based on the example
		if (Files.notExists(root.resolve("schema")))
			throw new IOException("Missing schema file");
		schema = FileChannel.open(root.resolve("schema"), READ, WRITE);
		
		if (Files.notExists(root.resolve("metadata")))
			throw new IOException("Missing metadata file");
		metadata = FileChannel.open(root.resolve("metadata"), READ, WRITE);
		
		
		
		
		}
		 catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		//measure the record width
		
		//readMetadata(); //based on example
	}

	@Override
	public void clear() {
		truncate();
		size = 0;
		fingerprint = 0;
		//reset the metadata
		writeMetaData();
		
	}
	
	public void writeMetaData(){
		
		
		  MappedByteBuffer buf;
		try {
			buf = metadata.map(READ_WRITE, 0, 4 * 2);
			buf.putInt(fingerprint); 
			buf.putInt(size);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
	public List<Object> readSchema() {
		MappedByteBuffer buf;
		ArrayList<Object> temp = new ArrayList<Object>();
		try {
			buf = schema.map(READ_WRITE, 0, 4*2 + (1+15+1)*15);
		

		System.out.println(buf.getInt()); // DUMMY COLUMN COUNT, WIDTH 4
		System.out.println(buf.getInt()); // DUMMY PRIMARY INDEX, WIDTH 4

		// DUMMY COLUMN 0: letter STRING PRIMARY
		// WIDTH: 1 + maximum name length + 1
		{
			var length = buf.get();
			var chars = new byte[length];
			buf.get(chars);
			System.out.println(new String(chars, UTF_8));
			buf.get(new byte[15 - chars.length]);

			temp.add(FieldType.valueOf(buf.get()));
		}

		// DUMMY COLUMN 1: order INTEGER
		// WIDTH: 1 + maximum name length + 1
		{
			var length = buf.get();
			var chars = new byte[length];
			buf.get(chars);
			System.out.println(new String(chars, UTF_8));
			buf.get(new byte[15 - chars.length]);

			temp.add(FieldType.valueOf(buf.get()));
		}

		// DUMMY COLUMN 2: vowel BOOLEAN
		// WIDTH: 1 + maximum name length + 1
		{
			var length = buf.get();
			var chars = new byte[length];
			buf.get(chars);
			System.out.println(new String(chars, UTF_8));
			buf.get(new byte[15 - chars.length]);

			temp.add(FieldType.valueOf(buf.get()));
		}
	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
	}
		return null;
	}
	
	private void truncate() {
		//resolve the state folder
		//if its not there yet, just quit this method
		//if there is a state folder, recursively delete the state folder using a walk (as in the example)
		var state = root.resolve("state");
			if(Files.exists(state)) {
				
					try {
						Files.walk(state)
							.sorted(Comparator.reverseOrder())
							.map(path -> path.toFile())
							.forEach(file -> file.delete());
						
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
					
					
				
					
					
					
				}
			
	}
			
				
			
	
	//optional for M2
	private void delete() {
		try {
		truncate();
		var s = root.resolve("schema");
			Files.delete(s);
		var met = root.resolve("metadata");
		Files.delete(met);
		Files.delete(root);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		var digest = digest(key);
		//get the digest for the key
		var row = pathOf(digest);
		if(!Files.notExists(row)) {
			return false;
		}
		
		else {
			try {
				Files.delete(row);
				//fingerprint = fingerprint - hashCode(digest);
				size--;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
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
	
	private void writeSchema() {
		try {
			var buf = schema.map(READ_WRITE, fingerprint, size);
			
			var name = root.getName(fingerprint).toString();
			var chars = name.getBytes(UTF_8);
			buf.put((byte) chars.length);
			buf.put(chars);
			buf.put(new byte[15 - chars.length]);

			var type = STRING;
			buf.put((byte) type.getTypeNumber());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void writeRecord(String digest, List<Object> row) {
		//fill in from example code
		var path = pathOf(digest);
		
		try {
			var channel = FileChannel.open(path, CREATE, READ, WRITE);
			var buffer = channel.map(READ_WRITE, fingerprint, size);
			
			// DUMMY FIELD 0 STRING
			// WIDTH: 1 + max string length
			{
				var str = "alpha"; // DUMMY LETTER VALUE
				var chars = str.getBytes(UTF_8);
				buffer.put((byte) chars.length);
				buffer.put(chars);
			}

			// DUMMY FIELD 1 INTEGER-AS-SHORT
			// WIDTH: 1 + 2
			{
				buffer.put((byte) Short.BYTES);
				buffer.putShort((short) 1); // DUMMY ORDER VALUE
			}

			// DUMMY FIELD 2 BOOLEAN
			// WIDTH: 1
			{
				buffer.put((byte) 1); // DUMMY VOWEL VALUE
			}
		

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		

		
	}
	
	private List<Object> readRecord(String digest){
		//fill in from example code
		ArrayList<Object> temp = new ArrayList<Object>();
		try {
			if (Files.notExists(pathOf(digest))) {
				return null;
			}
			else {
				
			
			var channel = FileChannel.open(pathOf(digest), READ);
			var buf = channel.map(READ_ONLY, 0, size());
			
			
			{
				var len = buf.get();
				var chars = new byte[len];
				buf.get(chars);
				temp.add(new String(chars, UTF_8)); // DUMMY LETTER VALUE
			}

			// DUMMY FIELD 1 INTEGER-AS-SHORT
			// WIDTH: 1 + 2
			{
				buf.get();
				temp.add(buf.getShort()); // DUMMY ORDER VALUE
			}

			// DUMMY FIELD 2 BOOLEAN
			// WIDTH: 1
			{
				temp.add(buf.get() == 1); // DUMMY VOWEL VALUE
			}
		
			return temp;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		String prim = digest.substring(0,2);
		String sec = digest.substring(2);
		//get the 2 prefix chars
		//get the 38 suffix chars
		
		//return a path from the root
		//which resolves the prefix
		//and then resolves the suffix
		var path = root.resolve("state").resolve(prim).resolve(sec);
		
		//return the resolved path
		return path;
	}
	
	// path 12/e45678abcdef -> "12345678abcdef"
	private String digestOf(Path path) {
		String folderName = "";
		String fileName = "";
		String concat;
		folderName = path.getParent().toString();
		fileName = path.getFileName().toString();
		concat = folderName + fileName;
		
		
		//get the folder name (asking the path object for its parents filename as a string)
		//get the file name (asking the path object for its filename as a string)
		//concatenate them together
		return concat;
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

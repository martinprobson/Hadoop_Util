package net.martinprobson.hadoop.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Utility methods for dealing with HDFS (and local) filesystems.
 * 
 * @author martinr
 *
 */
public class HDFSUtil {

    private static final Log LOG = LogFactory.getLog(HDFSUtil.class);

	/**
	 * Check if a path exists in the file system given by Configuration.
	 * @param conf - Hadoop confiuration referencing file system to use.
	 * @param name - Path name to check.
	 * @return True if Path exists, False otherwise.
	 */
	public static boolean pathExists(Configuration conf, Path name) {
		boolean rc = true;
		try {
			FileSystem.get(conf).getFileStatus(name);
		} catch (FileNotFoundException e) {
				rc = false;
		} catch (IOException e) {
			LOG.error("Error getFileStatus() " + name,e);
			System.exit(2);
		}
		String msg = rc == true ? name + " exists" : name + " does not exist";
		LOG.debug(msg);
		return rc;
	}

	/**
	 * Delete a path exists in the file system given by Configuration.
	 * @param conf - Hadoop configuration referencing file system to use.
	 * @param name - Path name to delete.
	 * @return True if Path exists, False otherwise.
	 */
	public static void deletePath(Configuration conf, Path name) {
		LOG.debug("Deleting " + name);
		try {
			FileSystem.get(conf).delete(name,true);
		} catch (IOException e) {
			LOG.error("Error deletePath() " + name,e);
			System.exit(2);
		}
	}
	
	//@TODO Re-factor
	/**
	 * Read a Hadoop SequenceFile.
	 * @param conf - Hadoop configuration referencing file system to use.
	 * @param path - file to be read (Path)
	 * @return List of Writable Pairs containing the key/value contents of the file.
	 * @throws IOException 
	 */
	public static List<Pair<Writable,Writable>> readSeqFile(Configuration conf, Path fileName) throws IOException {

		List<Pair<Writable,Writable>> lines = new ArrayList<>();
		
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf,SequenceFile.Reader.file(fileName));
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf);
			while (reader.next(key,value)) {
				Pair<Writable,Writable> line =  new ImmutablePair<>(WritableUtils.clone(key, conf),
																     WritableUtils.clone(value, conf));
				lines.add(line);
			}
			
		} finally {
			IOUtils.closeStream(reader);
		}
		
		return lines;
	}
	
	
	//@TODO Re-factor
	/**
	 * Read a file.
	 * @param conf - Hadoop configuration referencing file system to use.
	 * @param path - file to be read (Path)
	 * @return String representation of file.
	 */
	public static String readFile(Configuration conf, Path fileName) {
		StringBuilder lines = new StringBuilder();
		try {
			FSDataInputStream in = FileSystem.get(conf).open(fileName);   
			BufferedReader br = new BufferedReader(new InputStreamReader(in,Charset.defaultCharset()));			
			String tmp; 
			while ((tmp = br.readLine()) != null) 
				lines.append(tmp + "\n");
			br.close();
		} catch (IOException e) {
			LOG.error("Error reading file: " + fileName, e);
			System.exit(2);
		}
		return(lines.toString());
		
	}
	
	
	//@TODO Re-factor
	/**
	 * Read a file.
	 * @param conf - Hadoop configuration referencing file system to use.
	 * @param path - file to be read (Path)
	 * @return List<String> collection of lines from file.
	 */
	public static List<String> readFileasList(Configuration conf, Path fileName) {
		List<String> lines = new ArrayList<>();
		try {
			FSDataInputStream in = FileSystem.get(conf).open(fileName);   
			BufferedReader br = new BufferedReader(new InputStreamReader(in,Charset.defaultCharset()));			
			String tmp; 
			while ((tmp = br.readLine()) != null) 
				lines.add(tmp);
			br.close();
		} catch (IOException e) {
			LOG.error("Error reading file: " + fileName, e);
			System.exit(2);
		}
		return(lines);
		
	}
	
	
	/**
	 * Read a file from local filesystem
	 * @param path - file to be read (URI)
	 * @param encoding - charset encoding.
	 * @return String representation of file.
	 */
	public static String readLocalFile(URI path, Charset encoding) 
			  throws IOException 
			{
			  byte[] encoded = Files.readAllBytes(Paths.get(path));
			  return new String(encoded, encoding);
			}

	/**
	 * Read a file
	 * @param path - file to be read (String)
	 * @param encoding - charset encoding.
	 * @return String representation of file.
	 */
	public static String readLocalFile(String path, Charset encoding) 
			  throws IOException, URISyntaxException
	{
			  return readLocalFile(new URI(path),encoding);
	}

}

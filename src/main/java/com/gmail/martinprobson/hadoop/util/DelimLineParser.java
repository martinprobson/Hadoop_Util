/**
 * 
 */
package com.gmail.martinprobson.hadoop.util;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;


/**
 * Utility class to parse fields out of a delimited line.
 * <p>
 * <p>An optional name of a properties file can be supplied that maps field-names to field position 
 * allowing the use of {@link #getFieldByName getFieldByName} method. 
 * <p>The properties file supplied must be on the classpath and be in the format: -
 * <ul>
 * <li>field1=fieldposition1</li>
 * <li>field2=fieldposition2</li>
 * etc
 * </ul>
 * <p>If a properties file is not supplied, then fields can only be accessed via the 
 * {@link #getFieldByPosition getFieldByPosition} method.
 * <p>The field separator used can be set via {@link #setFieldSeparator setFieldSeparator} 
 * method, otherwise it defaults to the standard Hadoop <code>\u0001<\code>.  
 * 
 * @author martinr
 *
 */
public class DelimLineParser {
	
	private static final Log LOG = LogFactory.getLog(DelimLineParser.class);
	private Properties fieldNameLookup;
	private String fieldSeparator = "\u0001";
	
	private static Properties loadFieldNames(String propertiesFile) {
		Properties fields = new Properties();
		try {
			fields.load(DelimLineParser.class.getResourceAsStream(propertiesFile));
		} catch (IOException e) {
			LOG.fatal("Cannot load field properties from classpath", e);
			System.exit(2);
		}
		return fields;
	}
	
	
	/**
	 * Construct a new parser with the given propertiesFileName and fieldSep.
	 * @param propertiesFileName - The properties file holding field->position mappings.
	 * @param fieldSep - The field separator to use when spliting the line.
	 */
	public DelimLineParser(String propertiesFileName, String fieldSep) {
		if (propertiesFileName != null) {
			this.fieldNameLookup = loadFieldNames(propertiesFileName);
		}
		else
			this.fieldNameLookup = new Properties();
		if (fieldSep != null)
			setFieldSeparator(fieldSep);
	}
	
	/**
	 * Construct a new parser with the given propertiesFileName and default fieldSep.
	 * @param propertiesFileName - The properties file holding field->position mappings.
	 */
	public DelimLineParser(String propertiesFileName) {
		this(propertiesFileName,null);
	}
	

	/**
	 * Construct a new parser with the given propertiesFileName and default fieldSep.
	 * @param fieldProperties - The properties holding field->position mappings.
	 */
	public DelimLineParser(Properties fieldProperties) {
		this(fieldProperties,null);
	}

	/**
	 * Construct a new parser with the given propertiesFileName and fieldSep.
	 * @param fieldProperties - The properties holding field->position mappings.
	 * @param fieldSep - The field separator to use when spliting the line.
	 */
	public DelimLineParser(Properties fieldProperties, String fieldSep) {
		if (fieldProperties == null) {
			this.fieldNameLookup = new Properties();
		} 
		else {
			this.fieldNameLookup = fieldProperties;
		}
		if (fieldSep != null)
			setFieldSeparator(fieldSep);
	}

	
	/**
	 * Construct a new parser with no properties file and the default field separator.
	 * <p>As no properties file is supplied, fields can only be retrieved via
	 * {@link #getFieldByPosition getFieldByPosition}
	 * <p>Calls to {@link #getFieldByName getFieldByName} will throw a NoSuchElement Exception.
	 */
	public DelimLineParser() {
		this(new Properties(),null);
	}
	
	/**
	 * Return the contents of the field in position specified by fieldPosition.
	 * @param line - The line to parse.
	 * @param fieldPosition
	 * @return String contents of the field.
	 * @throws NoSuchElementException if field does not exist.
	 */
	public String getFieldByPosition(Text line,int fieldPosition) throws NoSuchElementException {
		
		String fields[] = line.toString().split(getFieldSeparator(),fieldPosition+1);
		if ((fieldPosition <= 0) || (fieldPosition-1 > fields.length)) {
			throw new NoSuchElementException("Field Position " + fieldPosition + " does not exist");
		}
		return fields[fieldPosition-1];
	}
	
	/**
	 * Return the contents of the field named by fieldName..
	 * @param line - The line to parse.
	 * @param fieldName
	 * @return String contents of the field.
	 * @throws NoSuchElementException if field does not exist.
	 */
	public String getFieldByName(Text line,String fieldName) throws NoSuchElementException {
		
		int fieldPosition;
		
		if ((fieldNameLookup.getProperty(fieldName.toLowerCase()) == null)) 
			throw new NoSuchElementException("Field: " + fieldName  + "does not exist");
		else 
			fieldPosition = Integer.parseInt(fieldNameLookup.getProperty(fieldName.toLowerCase()));
		return getFieldByPosition(line,fieldPosition);
	}
	
	/**
	 * Set the field separator to be used.
	 * @param fieldSeparator
	 */
	public void setFieldSeparator(String fieldSeparator) {
		this.fieldSeparator = fieldSeparator;
	}
	
	/**
	 * Get the current field separator.
	 * @return current field separator.
	 */
	public String getFieldSeparator() {
		return fieldSeparator;
	}

}

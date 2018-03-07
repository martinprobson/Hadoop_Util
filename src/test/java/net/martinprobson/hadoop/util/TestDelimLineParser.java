package net.martinprobson.hadoop.util;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

import java.io.File;
import java.nio.charset.Charset;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.martinprobson.hadoop.util.DelimLineParser;


public class TestDelimLineParser {
	
	private static Text testCase;
	private final static String PROP_FILE_NAME = "/dfkkop.properties";
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
		File file = new File(TestDelimLineParser.class.getResource("/TestDfkkopParser_test_case.txt").getFile());
		String contents = FileUtils.readFileToString(file,Charset.defaultCharset());
		testCase = new Text(contents);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testDelimLineParserPropsString() {
		Properties fieldProps = new Properties();
		fieldProps.put("field1","1");
		fieldProps.put("field2","2");
		
		DelimLineParser p = new DelimLineParser(fieldProps,"|");
		assertThat(p,instanceOf(DelimLineParser.class));
		assertTrue(p.getFieldSeparator().equals("|"));
	}

	@Test
	public final void testDelimLineParserProps() {
		Properties fieldProps = new Properties();
		fieldProps.put("field1","1");
		fieldProps.put("field2","2");
		
		DelimLineParser p = new DelimLineParser(fieldProps);
		assertThat(p,instanceOf(DelimLineParser.class));
		assertTrue(p.getFieldSeparator().equals("\u0001"));
	}
	
	@Test
	public final void testDelimLineParserStringString() {
		DelimLineParser p = new DelimLineParser(PROP_FILE_NAME,"|");
		assertThat(p,instanceOf(DelimLineParser.class));
		assertTrue(p.getFieldSeparator().equals("|"));
	}

	@Test
	public final void testDelimLineParserString() {
		DelimLineParser p = new DelimLineParser(PROP_FILE_NAME);
		assertThat(p,instanceOf(DelimLineParser.class));
		assertTrue(p.getFieldSeparator().equals("\u0001"));
	}

	@Test
	public final void testDelimLineParser() {
		DelimLineParser p = new DelimLineParser();
		assertThat(p,instanceOf(DelimLineParser.class));
		assertTrue(p.getFieldSeparator().equals("\u0001"));
	}


	@Test
	public final void testSetFieldSeparator() {
		DelimLineParser p = new DelimLineParser();
		p.setFieldSeparator("A");
		assertTrue(p.getFieldSeparator().equals("A"));
	}

	@Test
	public final void testGetFieldSeparator() {
		DelimLineParser p = new DelimLineParser();
		p.setFieldSeparator("A");
		assertTrue(p.getFieldSeparator().equals("A"));
	}
	
	@Test
	public final void testGetFieldByNameVKONT() {
		DelimLineParser p = new DelimLineParser(PROP_FILE_NAME);
		assertTrue("Field 18 - VKONT = '850002626475'",p.getFieldByName(testCase,"vkont").equals("850002626475"));
	}
	
	@Test 
	public final void testGetFieldByNameXANZA() {
		DelimLineParser p = new DelimLineParser(PROP_FILE_NAME);
		assertTrue("Field 30 - XANZA = 'X'",p.getFieldByName(testCase,"XANZA").equals("X"));
	}
	
	@Test 
	public final void testGetFieldByNameBLDAT() {
		DelimLineParser p = new DelimLineParser(PROP_FILE_NAME);
		assertTrue("Field 32 - BLDAT = '20120422'",p.getFieldByName(testCase,"bldat").equals("20120422"));
	}

	@Test 
	public final void testGetFieldByNameABWBL() {
		DelimLineParser p = new DelimLineParser(PROP_FILE_NAME);
		assertTrue("Field 19 - ABWBL = ''",p.getFieldByName(testCase,"ABWBL").equals(" "));
	}

	@Test(expected = NoSuchElementException.class) 
	public final void testGetFieldNotExists() {
		DelimLineParser p = new DelimLineParser(PROP_FILE_NAME);
		p.getFieldByName(testCase,"Not exists");
	}
	
	@Test(expected = NoSuchElementException.class) 
	public final void testNoPropertyFile() {
		DelimLineParser p = new DelimLineParser();
		p.getFieldByName(testCase,"Not exists");
	}
	
	@Test
	public final void testGetFieldByPositionVKONT() {
		DelimLineParser p = new DelimLineParser();
		assertTrue("Field 18 - VKONT = '850002626475'",p.getFieldByPosition(testCase,18).equals("850002626475"));
	}
	
	@Test 
	public final void testGetFieldByPositionXANZA() {
		DelimLineParser p = new DelimLineParser();
		assertTrue("Field 30 - XANZA = 'X'",p.getFieldByPosition(testCase,30).equals("X"));
	}
	
	@Test 
	public final void testGetFieldByPositionBLDAT() {
		DelimLineParser p = new DelimLineParser();
		assertTrue("Field 32 - BLDAT = '20120422'",p.getFieldByPosition(testCase,32).equals("20120422"));
	}

	@Test 
	public final void testGetFieldByPositionABWBL() {
		DelimLineParser p = new DelimLineParser();
		assertTrue("Field 19 - ABWBL = ''",p.getFieldByPosition(testCase,19).equals(" "));
	}
	
	@Test 
	public final void testGetLastFieldByPosition() {
		Properties fieldProps = new Properties();
		fieldProps.put("field1","1");
		fieldProps.put("field2","2");
		DelimLineParser p = new DelimLineParser(fieldProps,"\\|");
		assertTrue("Field 2 -  = 'field2'",p.getFieldByPosition(new Text("field1|field2"),2).equals("field2"));
	}

	@Test 
	public final void testGetFirstFieldByPosition() {
		Properties fieldProps = new Properties();
		fieldProps.put("field1","1");
		fieldProps.put("field2","2");
		DelimLineParser p = new DelimLineParser(fieldProps,"\\|");
		assertTrue("Field 1  = 'field1'",p.getFieldByPosition(new Text("field1|field2"),1).equals("field1"));
	}
	

	@Test(expected = NoSuchElementException.class) 
	public final void testGetFieldNotExistsByPosition() {
		DelimLineParser p = new DelimLineParser(PROP_FILE_NAME);
		p.getFieldByPosition(testCase,999);
	}
	
}

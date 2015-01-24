import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * The Handler for SAX Events.
 * contains the main method retrieveQueries that parses an xml and returns a List of QueryXml
 * 
 * implented based on the code at 
 * http://www.javacodegeeks.com/2013/05/parsing-xml-using-dom-sax-and-stax-parser-in-java.html
 * 
 * 
 * complicated bugs were found so not currently used, but this would be a better alternative
 */

class SAXQueryHandler extends DefaultHandler {
 
	
	/**
     * static method that uses SAX and the methods in this class to extract QueryXml objects from a set of queries
	 * @param xml
	 * @return
	 * @throws Exception
	 */
	public static ArrayList<QueryXml> retrieveQueries(String xml) throws Exception {
		
		    SAXParserFactory parserFactor = SAXParserFactory.newInstance();
		
		    SAXParser parser = parserFactor.newSAXParser();
		
		    SAXQueryHandler handler = new SAXQueryHandler();
		
		    parser.parse(new FileInputStream(new File(xml)),handler);
		    
		    return handler.queryList;
		  }

  ArrayList<QueryXml> queryList = new ArrayList<>();
  QueryXml query = null;
  String content = null;
  @Override
  //Triggered when the start of a tag is found.
  public void startElement(String uri, String localName,
                           String qName, Attributes attributes)
                           throws SAXException {
 
    switch(qName){
      //Create a new QueryXml object when the start tag is found
      case "top":
        query = new QueryXml();
        query.num = attributes.getValue("num");
        break;
    }
  }
 
  /**
   * triggered when an end tag is found
   * the querytime and querytweettime cases are currently ignored
   */
  @Override
  public void endElement(String uri, String localName,
                         String qName) throws SAXException {
   switch(qName){
     //Add the employee to list once end tag is found
     case "top":
       queryList.add(query);      
       break;
     //For all other end tags the employee has to be updated.
     case "num":
       query.num = content;
       break;
     case "title":
       query.num = content;
       break;
     case "querytime":
         
         break;
     case "querytweettime":
         
         break;
         
   }
  }

  /**
   * magic
   */
  @Override
  public void characters(char[] ch, int start, int length)
          throws SAXException {
    content = String.copyValueOf(ch, start, length).trim();
  }
}


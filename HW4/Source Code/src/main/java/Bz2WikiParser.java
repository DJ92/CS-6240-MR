import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Decompresses bz2 file and parses Wikipages on each line.
 * It parses each line from the corpus and returns the following:
 * -> The Set of Outlinks for the PageName in context
 */
public class Bz2WikiParser {

    //Initialize patterns
    private static Pattern namePattern;
    private static Pattern linkPattern;

    static {
        // Regex pattern to Keep only html pages not containing tilde (~).
        namePattern = Pattern.compile("^([^~]+)$");

        // Regex pattern to Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
    }

    public static Set<String> getLinkPages(String record) throws SAXException, ParserConfigurationException {

        //Set to store outlinks
        Set<String> linkPageNames = new HashSet<>();
        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = spf.newSAXParser();
            XMLReader xmlReader = saxParser.getXMLReader();

            //Split pageName and HTML
            int delimLoc = record.indexOf(':');
            //Store pageName
            String pageName = record.substring(0, delimLoc).trim();

            //Set of outlinks from the html:
            // Parser fills this list with linked page names.
            xmlReader.setContentHandler(new WikiParser(pageName,linkPageNames));

            //HTML for the page in context
            String html = record.substring(delimLoc + 1);

            //Replace "&" character to "&amp;" for succesfull XML parsing
            html = html.replaceAll("&", "&amp;");

            //Check for Valid PageName using the regex above
            Matcher matcher = namePattern.matcher(pageName);
            if (!matcher.find())
                return null;

            // Parse page and fill list of linked pages.
            linkPageNames.clear();
            try {
                //Pass to XMLReader for parsing links from HTML String
                xmlReader.parse(new InputSource(new StringReader(html)));
            } catch (Exception e) {
                // Discard ill-formatted pages.
                return null;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
        //return the set of outlinks for the page in context
        return linkPageNames;
    }

    /**
     * Parses a Wikipage, finding links inside bodyContent div element.
     */
    private static class WikiParser extends DefaultHandler {

        /**
         * PageName passed by the mehtod above
         * List of linked pages; filled by parser.
         */
        private String pageName;
        private Set<String> linkPageNames;

        /**
         * Nesting depth inside bodyContent div element.
         */
        private int count = 0;

        //Constructor
        public WikiParser(String pageName,Set<String> linkPageNames) {
            super();
            this.pageName = pageName;
            this.linkPageNames = linkPageNames;
        }
        //Function to handle Tag Start
        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            super.startElement(uri, localName, qName, attributes);
            if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
                // Beginning of bodyContent div element.
                count = 1;
            } else if (count > 0 && "a".equalsIgnoreCase(qName)) {
                // Anchor tag inside bodyContent div element.
                count++;
                //Get href field from the anchor tag
                String link = attributes.getValue("href");
                //Handle no link found
                if (link == null) {
                    return;
                }
                try {
                    // Decode escaped characters in URL.
                    link = URLDecoder.decode(link, "UTF-8");
                } catch (Exception e) {
                    // Wiki-weirdness; use link as is.
                }
                // Keep only html filenames ending relative paths and not containing tilde (~).
                Matcher matcher = linkPattern.matcher(link);
                if (matcher.find()) {
                    //Keep only pages that do not form a self link
                    //For Eg: PageName = "A", Link in the anchor tag = "A"
                    if(!matcher.group(1).equals(pageName))
                        linkPageNames.add(matcher.group(1).trim());
                }
            } else if (count > 0) {
                // Other element inside bodyContent div.
                count++;
            }
        }
        //Function to handle Tag End
        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);
            if (count > 0) {
                // End of element inside bodyContent div.
                count--;
            }
        }
    }
}

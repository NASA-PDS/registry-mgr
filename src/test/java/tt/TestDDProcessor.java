package tt;

import java.io.File;
import java.util.Map;
import java.util.TreeMap;

import gov.nasa.pds.registry.mgr.dd.LddEsJsonWriter;
import gov.nasa.pds.registry.mgr.dd.Pds2EsDataTypeMap;
import gov.nasa.pds.registry.mgr.dd.parser.AttributeDictionaryParser;
import gov.nasa.pds.registry.mgr.dd.parser.ClassAttrAssociationParser;
import gov.nasa.pds.registry.mgr.dd.parser.DDAttribute;


public class TestDDProcessor
{
    public static void main(String[] args) throws Exception
    {
        Pds2EsDataTypeMap dtMap = new Pds2EsDataTypeMap();
        dtMap.load(new File("src/main/resources/elastic/data-dic-types.cfg"));

        File ddFile = new File("src/test/data/PDS4_MSN_1B00_1100.JSON");
        File outFile = new File("/tmp/test.dd.json");
        
        // Parse and cache attributes
        Map<String, DDAttribute> ddAttrCache = new TreeMap<>();
        AttributeDictionaryParser attrParser = new AttributeDictionaryParser(ddFile, 
                (attr) -> { ddAttrCache.put(attr.id, attr); } );
        attrParser.parse();

        // Parse class attribute associations and write ES data file
        LddEsJsonWriter writer = new LddEsJsonWriter(outFile, dtMap, ddAttrCache);
        ClassAttrAssociationParser caaParser = new ClassAttrAssociationParser(ddFile, writer);
        caaParser.parse();
        writer.close();
        
        System.out.println();
        System.out.println("LDD Version: " + attrParser.getLddVersion());
        System.out.println("LDD Date:    " + attrParser.getLddDate());
    }

}

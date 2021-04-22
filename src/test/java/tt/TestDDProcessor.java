package tt;

import java.io.File;

import gov.nasa.pds.registry.mgr.dd.LddProcessor;
import gov.nasa.pds.registry.mgr.dd.Pds2EsDataTypeMap;
import gov.nasa.pds.registry.mgr.dd.parser.AttributeDictionaryParser;
import gov.nasa.pds.registry.mgr.dd.parser.ClassAttrAssociationParser;


public class TestDDProcessor
{
    public static void main(String[] args) throws Exception
    {
        File outFile = new File("/tmp/test.dd.json");

        Pds2EsDataTypeMap dtMap = new Pds2EsDataTypeMap();
        dtMap.load(new File("src/main/resources/elastic/data-dic-types.cfg"));
        
        //File ddFile = new File("/tmp/schema/PDS4_PDS_JSON_1E00.JSON");
        File ddFile = new File("/tmp/schema/PDS4_INSIGHT_1B00_1870.JSON");
        
        LddProcessor proc = new LddProcessor(outFile, dtMap, null);

        AttributeDictionaryParser parser1 = new AttributeDictionaryParser(ddFile, proc);
        parser1.parse();
        ClassAttrAssociationParser parser2 = new ClassAttrAssociationParser(ddFile, proc);
        parser2.parse();
        
        proc.close();
    }

}

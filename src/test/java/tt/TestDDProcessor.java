package tt;

import java.io.File;

import gov.nasa.pds.registry.mgr.dd.DDProcessor;
import gov.nasa.pds.registry.mgr.dd.parser.DDParser;

public class TestDDProcessor
{
    public static void main(String[] args) throws Exception
    {
        File outFile = new File("/tmp/test.dd.json");
        File typeMapFile = new File("src/main/resources/elastic/data-dic-types.cfg");
        
        File ddFile = new File("/tmp/schema/PDS4_PDS_JSON_1E00.JSON");
        
        DDProcessor proc = new DDProcessor(outFile, typeMapFile, null);

        DDParser parser = new DDParser();
        parser.parse(ddFile, proc);
        
        proc.processParentClasses();
        
        proc.close();
    }

}

package tt;

import java.io.File;

import gov.nasa.pds.registry.mgr.dd.parser.DDParser;
import gov.nasa.pds.registry.mgr.dd.parser.DDAttribute;
import gov.nasa.pds.registry.mgr.dd.parser.DDClass;


public class TestDDParser
{
    private static class MyCB implements DDParser.Callback
    {
        @Override
        public void onClass(DDClass claz) throws Exception
        {
            if(claz.parentId != null)
            {
                System.out.println(claz.getId() + "  -->  " + claz.parentId);
            }
        }

        
        @Override
        public void onAttribute(DDAttribute attr) throws Exception
        {
            System.out.println(attr.getId());
        }
    }
    
    
    public static void main(String[] args) throws Exception
    {
        //File file = new File("/tmp/schema/PDS4_PROC_1B00_1100.JSON");
        //File file = new File("/tmp/schema/PDS4_CART_1D00_1933.JSON");
        File file = new File("/tmp/schema/PDS4_PDS_JSON_1E00.JSON");
        
        MyCB cb = new MyCB();
        
        DDParser parser = new DDParser();
        parser.setParseAttributeDictionary(false);
        parser.parse(file, cb);
    }
}

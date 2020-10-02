package tt;

import java.io.File;

import gov.nasa.pds.registry.mgr.util.dd.DDParser;

public class TestDDParser
{
    public static void main(String[] args) throws Exception
    {
        DDParser parser = new DDParser();
        parser.parse(new File("/tmp/schema/PDS4_PROC_1B00_1100.JSON"), (attr) -> 
        {
            System.out.println(attr.id + "  -->  " + attr.dataType);
        });
    }
}

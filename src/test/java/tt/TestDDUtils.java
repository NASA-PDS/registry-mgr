package tt;

import java.io.File;
import java.util.Map;

import gov.nasa.pds.registry.mgr.dao.DDInfo;
import gov.nasa.pds.registry.mgr.dao.DDUtils;

public class TestDDUtils
{

    public static void main(String[] args) throws Exception
    {
        Map<String, DDInfo> map = DDUtils.createDataDictionaryMap(new File("/tmp/schema/pds2.csv"));
        map.forEach((ns, info) -> { 
            System.out.println(ns + ", " + info.url + ", " + info.version); 
        });
        
    }

}

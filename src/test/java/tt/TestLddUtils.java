package tt;

import java.io.File;
import java.util.Map;

import gov.nasa.pds.registry.mgr.dd.LddInfo;
import gov.nasa.pds.registry.mgr.dd.LddUtils;


public class TestLddUtils
{

    public static void main(String[] args) throws Exception
    {
        Map<String, LddInfo> map = LddUtils.loadLddList(new File("src/test/data/ldd_list.csv"));
        map.forEach((ns, info) -> { 
            System.out.println(ns + ", " + info.url + ", " + info.date); 
        });
        
    }

}

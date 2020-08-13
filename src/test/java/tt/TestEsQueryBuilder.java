package tt;

import gov.nasa.pds.registry.mgr.util.EsQueryBuilder;

public class TestEsQueryBuilder
{
    public static void main(String[] args) throws Exception
    {
        testMatchAll();
        System.out.println();
        
        testDelete();
        System.out.println();
        
        testUpdateStatus();
        System.out.println();
    }


    private static void testMatchAll() throws Exception
    {
        EsQueryBuilder bld = new EsQueryBuilder(true);
        String json = bld.createMatchAllQuery();
        System.out.println(json);
    }

    
    private static void testDelete() throws Exception
    {
        EsQueryBuilder bld = new EsQueryBuilder(true);
        String json = bld.createFilterQuery("lidvid", "test::1.0");
        System.out.println(json);
    }

    
    private static void testUpdateStatus() throws Exception
    {
        EsQueryBuilder bld = new EsQueryBuilder(true);
        String json = bld.createUpdateStatusJson("STAGED", "lidvid", "test::1.0");
        System.out.println(json);
    }
}

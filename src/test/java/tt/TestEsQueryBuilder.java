package tt;

import gov.nasa.pds.registry.mgr.util.EsQueryBuilder;

public class TestEsQueryBuilder
{
    public static void main(String[] args) throws Exception
    {
        EsQueryBuilder bld = new EsQueryBuilder(true);
        String json = bld.createUpdateStatusJson("STAGED", "lidvid", "test::1.0");
        System.out.println(json);
    }
}

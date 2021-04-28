package tt;

import gov.nasa.pds.registry.mgr.dao.SchemaRequestBld;

public class TestSchemaRequestBuilder
{
    public static void main(String[] args) throws Exception
    {
        testGetDDInfoRequest();
    }
    
    
    public static void testGetDDInfoRequest() throws Exception
    {
        SchemaRequestBld bld = new SchemaRequestBld(true);
        String req = bld.createGetDDInfoRequest("pds");
        System.out.println(req);
    }
}

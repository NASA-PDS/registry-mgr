package tt;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.dao.SchemaDAO;
import gov.nasa.pds.registry.mgr.es.client.EsClientFactory;


public class TestSchemaDAO
{

    public static void main(String[] args) throws Exception
    {
        testGetDataType();
    }

    
    private static void testIndexExists() throws Exception
    {
        RestClient client = EsClientFactory.createRestClient("localhost", null);
        SchemaDAO dao = new SchemaDAO(client);
        
        boolean b = dao.indexExists("t123");
        System.out.println(b);
        
        client.close();
    }
    
    
    private static void testGetFieldNames() throws Exception
    {
        RestClient client = EsClientFactory.createRestClient("localhost", null);
        SchemaDAO dao = new SchemaDAO(client);
        
        Set<String> names = dao.getFieldNames("t1");
        for(String name: names)
        {
            System.out.println(name);
        }
        
        client.close();
    }
    
    
    private static void testGetDataType() throws Exception
    {
        RestClient client = EsClientFactory.createRestClient("localhost", null);
        SchemaDAO dao = new SchemaDAO(client);
        
        List<String> ids = new ArrayList<>();
        ids.add("pds/Property_Map/pds/identifier");
        ids.add("pds/Property_Maps/pds/identifier");
        
        dao.getDataTypes("registry", ids);
        
        client.close();
    }
}

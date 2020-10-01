package tt;

import java.util.Set;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.dao.SchemaDAO;
import gov.nasa.pds.registry.mgr.es.client.EsClientFactory;


public class TestSchemaDAO
{

    public static void main(String[] args) throws Exception
    {
    }

    
    private static void testIndexExists() throws Exception
    {
        RestClient client = EsClientFactory.createRestClient("localhost", null);
        boolean b = SchemaDAO.indexExists(client, "t123");
        System.out.println(b);
        
        client.close();
    }
    
    
    private static void testGetFieldNames() throws Exception
    {
        RestClient client = EsClientFactory.createRestClient("localhost", null);
        
        Set<String> names = SchemaDAO.getFieldNames(client, "t1");
        for(String name: names)
        {
            System.out.println(name);
        }
        
        client.close();

    }
}

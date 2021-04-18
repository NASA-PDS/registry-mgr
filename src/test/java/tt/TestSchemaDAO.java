package tt;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.mgr.dao.SchemaDAO;
import gov.nasa.pds.registry.mgr.util.Tuple;
import gov.nasa.pds.registry.mgr.util.es.IndexUtils;


public class TestSchemaDAO
{

    public static void main(String[] args) throws Exception
    {
        testGetDataType();
    }

    
    private static void testIndexExists() throws Exception
    {
        RestClient client = EsClientFactory.createRestClient("localhost", null);
        
        boolean b = IndexUtils.indexExists(client, "t123");
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
        
        try
        {
            SchemaDAO dao = new SchemaDAO(client);
            
            Set<String> ids = new TreeSet<>();
            ids.add("pds:Property_Map/pds:identifier");
            ids.add("test");
            
            List<Tuple> results = dao.getDataTypes("registry", ids, (name) -> { return null; });
            for(Tuple res: results)
            {
                System.out.println(res.item1 + "  -->  " + res.item2);
            }
        }
        finally
        {
            client.close();
        }
    }
}

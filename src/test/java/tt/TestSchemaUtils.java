package tt;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.util.es.EsSchemaUtils;
import gov.nasa.pds.registry.mgr.util.es.EsUtils;

public class TestSchemaUtils
{

    public static void main(String[] args) throws Exception
    {
        RestClient client = EsUtils.createClient("localhost", null);
        boolean b = EsSchemaUtils.indexExists(client, "t123");
        System.out.println(b);
        
        client.close();
    }

}

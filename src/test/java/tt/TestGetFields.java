package tt;

import java.util.Set;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.util.EsClientBuilder;
import gov.nasa.pds.registry.mgr.util.EsUtils;

public class TestGetFields
{

    public static void main(String[] args) throws Exception
    {
        RestClient client = EsClientBuilder.createClient("localhost");
        
        Set<String> names = EsUtils.getFieldNames(client, "t1");
        for(String name: names)
        {
            System.out.println(name);
        }
        
        client.close();
    }

}

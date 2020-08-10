package tt;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.util.EsUtils;


public class TestRestClient
{

    public static void main(String[] args) throws Exception
    {
        HttpHost host = EsUtils.parseUrl("my-host");
        System.out.println(host);
        
        RestClient client = EsUtils.createClient("localhost");

        
        client.close();
    }

}

package gov.nasa.pds.registry.mgr.util;


import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;


public class EsUtils
{
    public static RestClient createClient(String url) throws Exception
    {
        HttpHost host = parseUrl(url);
        RestClient client = RestClient.builder(host).build();
        return client;
    }
    
    
    public static HttpHost parseUrl(String url) throws Exception
    {
        if(url == null) throw new Exception("URL is null");
        
        String tmpUrl = url.trim();

        String proto = "http";
        String host = null;
        int port = 9200;
        
        // Protocol
        int idx = tmpUrl.indexOf("://");
        if(idx > 0)
        {
            proto = tmpUrl.substring(0, idx).toLowerCase();
            if(!proto.equals("http") && !proto.equals("https")) 
            {
                throw new Exception("Invalid protocol '" + proto + "'. Expected 'http' or 'https'.");
            }
            
            tmpUrl = tmpUrl.substring(idx + 3);
        }
        
        // Host & port
        idx = tmpUrl.indexOf(":");
        if(idx > 0)
        {
            host = tmpUrl.substring(0, idx);
            
            // Port
            String strPort = tmpUrl.substring(idx + 1);
            idx = strPort.indexOf("/");
            if(idx > 0)
            {
                strPort = strPort.substring(0, idx);
            }
            
            try
            {
                port = Integer.parseInt(strPort);
            }
            catch(Exception ex)
            {
                throw new Exception("Invalid port " + strPort);
            }
        }
        // Host only
        else
        {
            host = tmpUrl;
            idx = host.indexOf("/");
            if(idx > 0)
            {
                host = host.substring(0, idx);
            }
        }
        
        HttpHost httpHost = new HttpHost(host, port, proto);
        return httpHost;
    }
}

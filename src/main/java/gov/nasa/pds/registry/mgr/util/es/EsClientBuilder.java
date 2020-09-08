package gov.nasa.pds.registry.mgr.util.es;

import java.util.Properties;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;


public class EsClientBuilder
{
    private RestClientBuilder bld;
    private ClientConfigCB clientCB;
    private RequestConfigCB reqCB;
    
    
    public EsClientBuilder(String url) throws Exception
    {
        HttpHost host = parseUrl(url);
        bld = RestClient.builder(host);
        
        clientCB = new ClientConfigCB();
        reqCB = new RequestConfigCB();
    }
    
    
    public RestClient build() 
    {
        bld.setHttpClientConfigCallback(clientCB);
        bld.setRequestConfigCallback(reqCB);
        
        return bld.build();
    }
    
    
    public void configureAuth(Properties props) throws Exception
    {
        if(props == null) return;

        if(Boolean.TRUE.equals(getBoolean(props, "trust.self-signed")))
        {
            clientCB.setTrustSelfSignedCert(true);
        }
    }
    
    
    private static Boolean getBoolean(Properties props, String key) throws Exception
    {
        if(props == null) return null;
        
        String str = props.getProperty(key);
        if(str == null) return null;

        if(!str.equals("true") && str.equals("false")) 
        {
            throw new Exception("Property " + key + " has invalid value " + str);
        }
        
        return str.equals("true");
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

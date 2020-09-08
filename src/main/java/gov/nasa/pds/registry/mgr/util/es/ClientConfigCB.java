package gov.nasa.pds.registry.mgr.util.es;

import javax.net.ssl.SSLContext;

import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClientBuilder;


public class ClientConfigCB implements RestClientBuilder.HttpClientConfigCallback
{
    private boolean trustSelfSignedCert = false;
    
    
    public ClientConfigCB()
    {
    }

    
    public void setTrustSelfSignedCert(boolean b)
    {
        this.trustSelfSignedCert = b;
    }

    
    @Override
    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder)
    {
        try
        {
            if(trustSelfSignedCert)
            {
                confTrustSelfSigned(httpClientBuilder);
            }
            
            return httpClientBuilder;
        }
        catch(Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    
    private void confTrustSelfSigned(HttpAsyncClientBuilder httpClientBuilder) throws Exception
    {
        SSLContextBuilder sslBld = SSLContexts.custom(); 
        sslBld.loadTrustMaterial(new TrustSelfSignedStrategy());
        SSLContext sslContext = sslBld.build();

        httpClientBuilder.setSSLContext(sslContext);
    }
}

package gov.nasa.pds.registry.mgr.util.ssl;

import java.security.SecureRandom;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;


public class SSLUtils
{
    public static void trustAllCerts() throws Exception
    {
        TrustManager[] trustManagers = new TrustManager[1];
        trustManagers[0] = new TrustAllManager();
        
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustManagers, new SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    }
}

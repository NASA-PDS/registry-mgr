package gov.nasa.pds.registry.mgr.util.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;

import gov.nasa.pds.registry.common.es.client.SSLUtils;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.Logger;


public class FileDownloader
{
    private int timeout = 5000;
    private int numRetries = 3;
    private boolean sslTrustAll = true;
    
    
    public FileDownloader()
    {
    }

    
    public void download(String fromUrl, File toFile) throws Exception
    {
        int count = 0;
        
        while(true)
        {
            try
            {
                count++;
                downloadOnce(fromUrl, toFile);
                return;
            }
            catch(Exception ex)
            {
                System.out.println("[ERROR] " + ex.getMessage());
                if(count < numRetries)
                {
                    Logger.info("Will retry in 5 seconds");
                    Thread.sleep(5000);
                }
                else
                {
                    throw new Exception("Could not download " + fromUrl);
                }
            }
        }
    }
    
    
    private void downloadOnce(String fromUrl, File toFile) throws Exception
    {
        InputStream is = null;
        FileOutputStream os = null;
        
        Logger.info("Downloading " + fromUrl);
        
        try
        {
            HttpURLConnection con = createConnection(new URL(fromUrl));
            os = new FileOutputStream(toFile);
            
            is = con.getInputStream();
            is.transferTo(os);
        }
        finally
        {
            CloseUtils.close(os);
            CloseUtils.close(is);
        }
    }


    private HttpURLConnection createConnection(URL url) throws Exception
    {
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        if(con instanceof HttpsURLConnection)
        {
            HttpsURLConnection tlsCon = (HttpsURLConnection)con;

            if(sslTrustAll)
            {
                // Trust invalid / self-signed certificates
                SSLContext sc = SSLUtils.createTrustAllContext();
                tlsCon.setSSLSocketFactory(sc.getSocketFactory());

                // Do not verify host name (CN=host)
                tlsCon.setHostnameVerifier(new HostnameVerifier()
                {
                    @Override
                    public boolean verify(String hostname, SSLSession session)
                    {
                        return true;
                    }
                });
            }
        }
        
        con.setConnectTimeout(timeout);
        con.setReadTimeout(timeout);
        con.setAllowUserInteraction(false);
        
        return con;
    }

}

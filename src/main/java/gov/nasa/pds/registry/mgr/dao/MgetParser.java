package gov.nasa.pds.registry.mgr.dao;

import java.io.IOException;
import org.apache.http.HttpEntity;


public class MgetParser
{
    public static interface Callback
    {
        public void onId(String id);
        public void onFound(boolean b);
        public void onSource(Object src);
    }
    

    /////////////////////////////////////////////////////////////////////
    
    public MgetParser()
    {
    }
    
    
    public void parse(HttpEntity entity, Callback cb) throws IOException
    {
        
    }
}

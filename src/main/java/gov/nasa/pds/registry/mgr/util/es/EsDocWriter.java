package gov.nasa.pds.registry.mgr.util.es;

import java.io.Closeable;
import java.io.File;

public class EsDocWriter implements Closeable, SearchResponseParser.Callback
{
    public EsDocWriter(File file)
    {
    }

    
    @Override
    public void close()
    {
    }


    @Override
    public void onRecord(String id, Object rec)
    {
        System.out.println(id);
        System.out.println(rec);
        System.out.println();
    }
}

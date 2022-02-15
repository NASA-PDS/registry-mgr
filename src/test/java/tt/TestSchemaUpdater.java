package tt;

import java.io.File;
import gov.nasa.pds.registry.mgr.cfg.RegistryCfg;
import gov.nasa.pds.registry.mgr.dao.RegistryManager;
import gov.nasa.pds.registry.mgr.dao.schema.SchemaUpdater;


public class TestSchemaUpdater
{

    public static void main(String[] args) throws Exception
    {
        testUpdateLdds();
    }
    
    
    public static void testUpdateLdds() throws Exception
    {
        
        RegistryCfg cfg = new RegistryCfg();
        cfg.url = "http://localhst:9200";
        cfg.indexName = "registry";
        
        RegistryManager.init(cfg);
        
        try
        {
            SchemaUpdater updater = new SchemaUpdater(cfg, false);
            updater.updateLdds(new File("/tmp/harvest/out/missing_xsds.txt"));
        }
        finally
        {
            RegistryManager.destroy();
        }
    }
    
}

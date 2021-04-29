package gov.nasa.pds.registry.mgr.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.dd.LddInfo;
import gov.nasa.pds.registry.mgr.dd.LddUtils;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.Logger;
import gov.nasa.pds.registry.mgr.util.file.FileDownloader;


/**
 * This class adds new fields to Elasticsearch "registry" index 
 * by calling Elasticsearch schema API. 
 * 
 * The list of field names is read from a file. For all fields not in 
 * current "registry" schema, the data type is looked up in data dictionary 
 * index ("registry-dd").
 * 
 * If a field definition is not available in the data dictionary index,
 * the latest version of LDD will be downloaded if needed.
 * 
 * @author karpenko
 */
public class SchemaUpdater
{
    private SchemaDAO dao;

    private Map<String, LddInfo> remoteLddMap;
    private Set<String> esFieldNames;
    
    private Set<String> batch;
    private int totalCount;
    private int batchSize = 100;
    
    private SchemaUpdaterConfig cfg;

    private FileDownloader downloader = new FileDownloader();
    
    /**
     * Constructor 
     * @param cfg Registry manager configuration
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name
     * @throws Exception
     */
    public SchemaUpdater(RestClient client, SchemaUpdaterConfig cfg) throws Exception
    {
        this.cfg = cfg;
        this.dao = new SchemaDAO(client);
        
        // Get a list of existing field names from Elasticsearch
        this.esFieldNames = dao.getFieldNames(cfg.indexName);
        
        this.batch = new TreeSet<>();
    }

    
    /**
     * Add new fields to Elasticsearch "registry" index. 
     * @param file A file with a list of fields to add.
     * @throws Exception
     */
    public void updateSchema(File file) throws Exception
    {
        List<String> newFields = getNewFields(file);

        totalCount = 0;
        batch.clear();

        for(String newField: newFields)
        {
            addField(newField);
        }

        finish();
        System.out.println("Updated " + totalCount + " fields");
    }
    
    
    private void addField(String name) throws Exception
    {
        if(esFieldNames.contains(name)) return;
        
        // Add field request to the batch
        batch.add(name);
        totalCount++;

        // Commit if reached batch/commit size
        if(totalCount % batchSize == 0)
        {
            updateSchema(cfg.indexName, batch);
            batch.clear();
        }
    }
    
    
    private void finish() throws Exception
    {
        if(batch.isEmpty()) return;
        updateSchema(cfg.indexName, batch);
    }
    
    
    private void updateSchema(String index, Set<String> batch) throws Exception
    {
        DataTypesInfo info = dao.getDataTypes(index, batch, false);
        if(info.lastMissingField == null) 
        {
            dao.updateSchema(index, info.newFields);
            return;
        }
        
        // Some fields are missing. Update LDDs if needed.
        boolean updated = updateLdds(info.missingNamespaces);
        
        // LDDs are up-to-date or LDD list is not available
        if(!updated) throw new DataTypeNotFoundException(info.lastMissingField);
        
        // LDDs were updated. Reload last batch. Stop (throw exception) on first missing field.
        info = dao.getDataTypes(index, batch, true);
        dao.updateSchema(index, info.newFields);
    }
    
    
    public boolean updateLdds(Set<String> namespaces)
    {
        // Load LDD list if needed
        if(cfg.lddCfgUrl == null) return false;
        try
        {
            loadLddList();
        }
        catch(Exception ex)
        {
            Logger.warn("Could not load list of data dictionaries. " 
                    + "Automatic data dictionary updates are not available.");
            return false;
        }
        
        /*
        LddInfo info = remoteLddMap.get(ns);
        if(info == null)
        {
            Logger.warn("No LDD for namespace '" + ns + "'");
            return null;
        }
        */
        
        return false;
    }

    
    private void loadLddList() throws Exception
    {
        if(remoteLddMap != null) return;

        File file = new File(cfg.tempDir, "pds_registry_ldd_list.csv");
        downloader.download(cfg.lddCfgUrl, file);

        remoteLddMap = LddUtils.loadLddList(file);
    }

    
    private static List<String> getNewFields(File file) throws Exception
    {
        List<String> fields = new ArrayList<>();
        
        BufferedReader rd = new BufferedReader(new FileReader(file));
        try
        {
            String line;
            while((line = rd.readLine()) != null)
            {
                line = line.trim();
                if(line.length() == 0) continue;
                fields.add(line);
            }
        }
        finally
        {
            CloseUtils.close(rd);
        }
        
        return fields;
    }

}

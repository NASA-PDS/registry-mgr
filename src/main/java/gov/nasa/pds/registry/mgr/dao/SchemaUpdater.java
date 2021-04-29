package gov.nasa.pds.registry.mgr.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.dd.LddInfo;
import gov.nasa.pds.registry.mgr.dd.LddLoader;
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
    private static final String WARN_LDD_NA = "Could not load list of LDDs. Automatic data dictionary updates are not available.";
    
    private SchemaDAO dao;

    private Map<String, LddInfo> remoteLddMap;
    private Map<String, Instant> localLddMap = new TreeMap<>();
    
    private Set<String> esFieldNames;
    
    private Set<String> batch;
    private int totalCount;
    private int batchSize = 100;
    
    private SchemaUpdaterConfig cfg;

    private FileDownloader fileDownloader = new FileDownloader();
    private LddLoader lddLoader;
    
    /**
     * Constructor 
     * @param cfg Registry manager configuration
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name
     * @throws Exception
     */
    public SchemaUpdater(RestClient client, LddLoader lddLoader, SchemaUpdaterConfig cfg) throws Exception
    {
        this.cfg = cfg;
        this.dao = new SchemaDAO(client);
        this.lddLoader = lddLoader;
        
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
    
    
    public boolean updateLdds(Set<String> namespaces) throws Exception
    {
        if(namespaces == null || namespaces.isEmpty()) return false;
        
        // Load LDD list if needed
        if(cfg.lddCfgUrl == null) return false;
        try
        {
            loadLddList();
        }
        catch(Exception ex)
        {
            Logger.warn(WARN_LDD_NA);
            return false;
        }
        
        boolean updated = false;
        
        for(String namespace: namespaces)
        {
            LddInfo remoteLdd = remoteLddMap.get(namespace);
            if(remoteLdd == null || remoteLdd.date == null) continue;

            // Get local LDD date
            if(!localLddMap.containsKey(namespace))
            {
                Instant date = dao.getLddDate(cfg.indexName, namespace);
                if(date == null) date = Instant.MIN;
                localLddMap.put(namespace, date);
            }

            Instant localDate = localLddMap.get(namespace);
            Instant remoteDate = remoteLdd.date;
            
            // Load the latest version of remote LDD
            if(localDate.isBefore(remoteDate))
            {
                String fileName = getFileNameFromUrl(remoteLdd.url);
                File lddFile = new File(cfg.tempDir, fileName);
                
                fileDownloader.download(remoteLdd.url, lddFile);
                lddLoader.load(lddFile, namespace);
                localLddMap.put(namespace, remoteDate);
                updated = true;
            }
        }
        
        return updated;
    }

    
    private void loadLddList() throws Exception
    {
        if(remoteLddMap != null) return;

        File file = new File(cfg.tempDir, "pds_registry_ldd_list.csv");
        fileDownloader.download(cfg.lddCfgUrl, file);

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

    
    private static String getFileNameFromUrl(String url)
    {
        if(url == null) return null;
        
        int idx = url.lastIndexOf('/');
        if(idx < 0) return url;
        
        return url.substring(idx+1);
    }

}

package gov.nasa.pds.registry.mgr.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.elasticsearch.client.RestClient;

import com.opencsv.CSVReader;

import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.file.FileDownloader;


/**
 * Updates Elasticsearch schema by calling Elasticsearch schema API  
 * @author karpenko
 */
public class SchemaUpdater implements SchemaDAO.MissingDataTypeCallback
{
    private SchemaDAO dao;

    private Map<String, DDInfo> ddRepo;
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

    
    public void updateSchema(File file) throws Exception
    {
        List<String> newFields = getNewFields(file);
        updateSchema(newFields);
    }
    
    
    /**
     * Add fields from data dictionary to Elasticsearch schema. Ignore existing fields.
     * @param dd
     * @throws Exception
     */
    public void updateSchema(List<String> newFields) throws Exception
    {
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
            dao.updateMappings(cfg.indexName, batch, this);
            batch.clear();
        }
    }
    
    
    private void finish() throws Exception
    {
        if(batch.isEmpty()) return;
        dao.updateMappings(cfg.indexName, batch, this);
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


    @Override
    public String getDataType(String fieldId)
    {
        // Automatically assign data type for known fields
        if(fieldId.startsWith("ref_lid_") || fieldId.startsWith("ref_lidvid_") 
                || fieldId.endsWith("_Area")) return "keyword";
        
        // Get field namespace
        String ns = getNamespace(fieldId);
        if(ns == null) return null;
        
        // Load list of data dictionaries if needed
        if(cfg.dataDictionaryRepoUrl == null) return null;
        try
        {
            loadDataDictionaryRepo();
        }
        catch(Exception ex)
        {
            System.out.println("[WARN] Could not load list of data dictionaries. " 
                    + "Automatic data dictionary updates are not available.");
            return null;
        }
        
        return null;
    }

    
    private void loadDataDictionaryRepo() throws Exception
    {
        if(ddRepo != null) return;

        File file = new File(cfg.tempDir, "pds_registry_dd_repo.csv");
        downloader.download(cfg.dataDictionaryRepoUrl, file);
        
        ddRepo = new TreeMap<>();
        
    }
    
    
    private static String getNamespace(String fieldId)
    {
        if(fieldId == null) return null;
        
        int idx = fieldId.indexOf(':');
        if(idx < 1) return null;
        
        return fieldId.substring(0, idx);
    }
}

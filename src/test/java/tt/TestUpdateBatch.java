package tt;

import gov.nasa.pds.registry.mgr.schema.UpdateBatch;

public class TestUpdateBatch
{

    public static void main(String[] args) throws Exception
    {
        UpdateBatch batch = new UpdateBatch(true);
        batch.addField("abc", "keyword");
        batch.addField("int123", "integer");
        String json = batch.closeAndGetJson();
        
        System.out.println(json);
    }

}

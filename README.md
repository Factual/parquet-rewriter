# TLDR
Parquet-rewriter is a way to update parquet files by rewriting them in a more efficient manner. Parquet-rewriter accomplishes this by only serializing/deserializing dirty row groups (ones that contain upserts or deletes) and passing others through to the destination file in their raw, unmodified form. This can be significantly faster than rewriting your parquet file from scratch.
 
# Background


Historically Parquet files have been viewed as immutable, and for good reason. You incur significant costs for structuring, compressing and writing out a parquet file. It is better to append data via new parquet files rather than incur the cost of a complete rewrite. This works well with event based datasets where redundancy and duplicates can be tolerated. However, when you are dealing with transactional recordsets or aggregate data, then redundant or obsolete records can become a problem. Parquet-rewriter provides a potentially cheaper alternative to completely rewriting your parquet files whenever you need to update these types of recordsets. 

# Detials

Parquet horizontally partitions sets of rows into row groups as depicted by this diagram:

![alt text](https://camo.githubusercontent.com/0f0b52f7405720585ed7303c9ff317f272ebba19/68747470733a2f2f7261772e6769746875622e636f6d2f6170616368652f706172717565742d666f726d61742f6d61737465722f646f632f696d616765732f46696c654c61796f75742e676966)

Each row group is completely independent, and row group locations and statistics are stored at the trailing end of the file. parquet-rewriter takes advantage of these characteristics and its update strategy revolves around mutating only dirty row groups, ones that contain new, deleted or updated records, and passing through unmodified row groups in their raw and already compressed form. 

![alt](https://factual.github.io/parquet-rewriter/img/workflow_smaller.png)



parquet-rewriter makes the following assumptions: First, all rows in each parquet file / shard need to be sorted by a primary key column. Secondly, all updates need to be applied using the same sort order. 

# Usage Example

```java
/*Let start out with a thrift structure of the type:
...
struct Person {
  1: required string uuid,
  2: required Name name,
  3: optional i32 age,
  4: Address address,
  5: string info
}

// an update record can be either an upsert 
// or a delete, in which case we just pass in the uuid
union Update { 
  // set person if you want an upsert
  1: Person person,
  // set uuid if you want a delete
  2: string uuid
}

We are going to use uuid as our key field and the parquet records will be sorted and partitioned into N shards

Assuming you run an update in parallel, and for each shard you call an update function that takes an input 
path and an output path
*/

void updateParquetFile(Configuration conf,Path sourceFile,ArrayList<Update> sortedUpdateRecords,Path destFile) {
    // this maps Person object to uuid (the key field)
    ParquetRewriter.KeyAccessor<Person,Binary>  keyAccessor = (person) -> { return Binary.fromConstantByteArray(person.getUuid().getBytes()); };
    
    // construct rewriter object 
    ParquetRewriter<Person,Binary> rewriter 
        = new ParquetRewriter<Person,Binary>(
            conf,
            sourceFile,
            destFile,
            new ThriftReadSupport<Person>(Person.class),
            new ThriftWriteSupport<Person>(Person.class),
            rowGroupSize,
            keyAccessor,
            ColumnPath.get("uuid"));    
    try { 
        // now walk updates ...
        for (Update update : updates) { 
            // if this is an upsert ... 
            if (update.isSetPerson()) { 
                rewriter.appendRecord(update.getPerson());
            }
            else { 
                rewriter.deleteRecordByKey(
                    Binary.fromConstantByteArray(updates.getUuid().getBytes()));
            }
        }
    }
    finally { 
        rewriter.close();
    }
}


```

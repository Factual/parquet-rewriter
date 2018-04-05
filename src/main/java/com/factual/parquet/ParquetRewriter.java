package com.factual.parquet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.util.HadoopStreams;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.parquet.hadoop.ParquetOutputFormat.*;

/**
 *
 * Primary public facing class. You instantiate on of these for each parquet
 * file you want to mutate. You then seek to each record you want to mutate and
 * either apply a DELETE mutation or an UPSERT mutation.
 *
 * IMPORTANT ASSUMPTIONS: You presorted your parquet file using a non-duplicating
 * primary key and using the sort order parquet implements (what you would expect
 * for primitive types and signed lexicographical sorting for binary keys.
 *
 * Created by rana
 */
public class ParquetRewriter<T,KT extends Comparable<KT>> implements Closeable{

  static Logger LOG = Logger.getLogger(ParquetRewriter.class);


  /**
   * A function that maps a structure (thrift/proto-buf obj) to a scalar key value
   * @param <T> structure type
   * @param <KT> key type (scalar)
   */
  @FunctionalInterface
  public static interface KeyAccessor<T,KT extends Comparable<KT>> {
    public KT getKeyForObj(T obj) throws IOException;
  }

  Configuration conf;
  KeyAccessor<T,KT> keyAccessor;
  ColumnPath keyPath;
  ParquetFileReader reader;
  ParquetFileWriter writer;
  ArrayList<BlockMetaData>      incomingBlocks;
  ArrayList<Statistics<KT>>   incomingStats = Lists.newArrayList();
  ReadSupport<T> readSupport;
  WriteSupport<T> writeSupport;
  ParquetBlockMutator<T,KT> activeMutableBlock;
  int nextBlockIndex = 0;
  KT lastKey;
  FSDataInputStream incomingStream;
  long blockSize;
  long rowGroupSize;

  /**
   *
   * Construct a new parquet-rewriter instance
   *
   * @param incomingConf Hadoop Configuration object
   * @param sourceFile Source parquet file to modify (input)
   * @param destFile Filename + path of modified parquet file (output)
   * @param readSupport ReadSupport to deserialize objects from columns to type (thrift/proto to columns)
   * @param writeSupport WriteSupport to serialize object from type to columns (columns to thrift/proto)
   * @param optionalRowGroupSize Optional row group size if you don't want to use avg row group size of source
   *                             (use -1 to specify none)
   * @param keyAccessor function that maps object (thrift/proto) to key (scalar value)
   * @param keyPath ColumnPath representing the location of the key(scalar) value in the parquet schema
   * @throws IOException
   */
  public ParquetRewriter(Configuration incomingConf,
                         Path sourceFile,
                         Path destFile,
                         ReadSupport<T> readSupport,
                         WriteSupport<T> writeSupport,
                         int optionalRowGroupSize,
                         KeyAccessor<T,KT> keyAccessor,
                         ColumnPath keyPath) throws IOException {

    this.conf = new Configuration(incomingConf);
    this.conf.setBoolean("parquet.strings.signed-min-max.enabled", true);
    this.keyAccessor = keyAccessor;
    this.keyPath = keyPath;
    this.reader = ParquetFileReader.open(conf,sourceFile);
    FileSystem fs = sourceFile.getFileSystem(conf);
    this.incomingStream = fs.open(sourceFile);
    this.incomingBlocks = new ArrayList<>(this.reader.getRowGroups());
    this.readSupport = readSupport;
    this.writeSupport = writeSupport;

    // calculate average block size ...
    long avgBlockSize = (long) incomingBlocks.stream().mapToLong(b -> b.getTotalByteSize()).average().getAsDouble();
    long avgRowGroupSize = (long) incomingBlocks.stream().mapToLong(b -> b.getRowCount()).average().getAsDouble();
    // see if block size defined in conf ... if not use avergage block size of incoming file ...
    this.blockSize = conf.getLong(BLOCK_SIZE, avgBlockSize);
    this.rowGroupSize = (optionalRowGroupSize  == -1) ? avgRowGroupSize : optionalRowGroupSize;


    this.writer = new ParquetFileWriter(conf,this.reader.getFileMetaData().getSchema(),destFile, ParquetFileWriter.Mode.CREATE,
            rowGroupSize,conf.getInt(MAX_PADDING_BYTES, 0));

    this.writer.start();


    loadStats();
  }

  /** after mutating a parquet file, you must close the rewriter to properly flush
   * (generated) output file
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {

    // flush any mutated blocks
    if (isMutableBlockActive()) {
      flushMutatedBlock();
    }
    // skip remaining blocks (write them back)
    while (isNextBlockAvailable()) {
      skipNextBlock();
    }

    this.writer.end(Maps.newHashMap());

    if (incomingStream  != null) {
      incomingStream.close();
    }
  }

  /**
   * Append the typed object to the file.
   * NOTE: The expectation is the file is ordered based on key field,
   * that this object contains that key field, and all appends are done in
   * proper sort order .
   *
   * @param recordObj (thrift/proto buf etc. object)
   * @throws IOException
   */
  public void appendRecord(T recordObj) throws IOException {
    // seek to relevant key ...
    seekToKey(keyAccessor.getKeyForObj(recordObj));
    // write record
    if (!isMutableBlockActive()) {
      throw new IOException("Mutable Block not Active");
    }
    else {
      activeMutableBlock.appendRecord(recordObj);
    }
  }

  /**
   * delete the given object. assumption is object has a proper key, and delete is applied
   * in proper sorted order.
   *
   * @param objToDelete
   * @throws IOException
   */
  public void deleteRecord(T objToDelete)throws IOException {
    deleteRecordByKey(keyAccessor.getKeyForObj(objToDelete));
  }

  /**
   * Delete the parquet row at the given key. Same assumptions as append:
   * File must be sorted on key. Deletes / Appends must come in proper order!!
   *
   * @param key
   * @throws IOException
   */
  public void deleteRecordByKey(KT key) throws IOException {
    // seek to relevant key
    seekToKey(key);
    // and do nothing since this will yield skip
  }

  /**
   * Load the next row group in the source stream and make it mutable.
   * This is an advanced method. You would use it ONLY in a circumstance where
   * you want to transform source file's row group sizing.
   * You would do this by getting into a
   *  while(isNextBlockAvailable()) {loadAndMutateNextBlock();flushMutatedBlock(); } loop.
   *
   * @throws IOException
   */
  public void loadAndMutateNextBlock() throws IOException {
    if (isMutableBlockActive())
      throw new IOException("loadAndMutateNextBlock called but existing block already active!");

    Statistics<KT> nextBlockStats = getNextBlockStats();
    LOG.trace("Exploding Block with Min:"+ nextBlockStats.genericGetMin() + " Max:" + nextBlockStats.genericGetMax());

    // load next row group
    PageReadStore pageReadStore = reader.readNextRowGroup();
    // and pass it along to mutator ...
    activeMutableBlock = new ParquetBlockMutator<>(conf,this.rowGroupSize,this.blockSize,pageReadStore,reader.getFileMetaData(),readSupport,writeSupport,writer,keyAccessor);
    // advance next block cursor
    nextBlockIndex++;
  }

  /**
   * Advanced method: have we reached the end of the stream (no more row groups available)?
   *
   * @return true if no more blocks available in source stream
   */
  public boolean isNextBlockAvailable() {
    return nextBlockIndex < incomingBlocks.size();
  }


  /**
   * Advanced method: flush the active mutated block to the stream
   *
   * @throws IOException
   */
  public void flushMutatedBlock() throws IOException {
    flushMutatedBlockInternal();
  }

  ////////////////////////////////////////////////////////////////////////
  // INTERNALS IMPLEMENTATION
  ////////////////////////////////////////////////////////////////////////
  private void loadStats() throws IOException {

    // iterate each block, and extract column stats for it
    incomingBlocks.stream().forEach( b -> {
      Optional<ColumnChunkMetaData> match = b.getColumns().stream().filter(c -> c.getPath().equals(keyPath)).findFirst();
      if (match.isPresent() && match.get().getStatistics() != null) {
        incomingStats.add(match.get().getStatistics());
      }
      else {
        throw new RuntimeException("Key Column Stats not found for block:"+ b + " keyPath:"+ keyPath);
      }
    });
  }

  void seekToKey(KT key) throws IOException{
    LOG.trace("Seeking to key:" + key);
    // sanity check .... cannot seek backwards ...
    if (lastKey() != null && key.compareTo(lastKey()) <= 0) {
      throw new IOException("Seek Key:" + key + " is less than or equal to lastKey:" + lastKey());
    }

    // start walking blocks
    while(isNextBlockAvailable()) {
      // get stats for next block ...
      Statistics<KT> nextBlockStats = getNextBlockStats();

      boolean keyGTEQNextBlockKey = (key.compareTo(nextBlockStats.genericGetMin()) >= 0);

      // if this key is >= to next block's key....
      if (keyGTEQNextBlockKey) {

        // if there is a mutable block active ... flush it ...
        if (isMutableBlockActive())
          flushMutatedBlock();

        // check to see if next block can encompass this key
        if (key.compareTo(nextBlockStats.genericGetMax()) <= 0) {
          // yes... explode this block
          loadAndMutateNextBlock();
          // done break out ...
          break;
        } else {
          LOG.trace("Skipping next block");
          skipNextBlock();
        }
      }
      else {
        LOG.trace("Key is LTNextBlockMinKey");
        break;
      }
    }
    // now if we get here and there is no active block ...
    if (!isMutableBlockActive()) {
      // then activate a new mutable block for this key ...
      activeNewMutableBlock();
    }
    else {
      getActiveMutableBlock().readRecord(key);
    }

    // update last key ...
    setLastKey(key);
  }

  void activeNewMutableBlock() throws IOException {
    if (isMutableBlockActive())
      throw new IOException("Existing Mutable Block Active!");
    // and pass it along to mutator ...
    activeMutableBlock = new ParquetBlockMutator<>(conf,this.rowGroupSize, this.blockSize,null,reader.getFileMetaData(),readSupport,writeSupport,writer,keyAccessor);
  }



  private void skipNextBlock() throws IOException {
    if (isMutableBlockActive())
      throw new IOException("skipNextBlock called but existing block already active!");

    // write out next row group directly to file
    writer.appendRowGroup(HadoopStreams.wrap(incomingStream),incomingBlocks.get(nextBlockIndex),false);
    // advance next block cursor
    nextBlockIndex++;
    // and advance reader ...
    reader.skipNextRowGroup();
  }


  /**
   * is a block mutator active ?
   *
   * @return true if mutable block is active
   */
  boolean isMutableBlockActive() {
    return activeMutableBlock != null;
  }

  /**
   * get access to the active block mutator
   *
   * @return
   */
  ParquetBlockMutator<T,KT> getActiveMutableBlock() {
    return activeMutableBlock;
  }

  /**
   * this is a package private version of the above method, and is used internally to collect stats
   *
    * @return
   * @throws IOException
   */
  List<Long> flushMutatedBlockInternal() throws IOException {
    activeMutableBlock.close();
    List<Long> statsOut = Lists.newArrayList(activeMutableBlock.totalReadTime,activeMutableBlock.totalWriteTime,
            activeMutableBlock.recordWriterRecordConsumerFlushTime,
            activeMutableBlock.recordWriterColumnIOFlushTime,
            activeMutableBlock.recordWriterFileIOFlushTime,
            activeMutableBlock.pageWriteTime,
            activeMutableBlock.dictionaryWriteTime);
    activeMutableBlock = null;
    return statsOut;
  }


  /**
   * update last written key
   *
   * @param key
   * @throws IOException
   */
  void setLastKey(KT key)throws IOException {
    if (!isMutableBlockActive()) {
      throw new IOException("Mutable Block not Active");
    }
    lastKey = key;
  }

  /**
   * get last written key
   * @return
   */
  KT lastKey() {
    return lastKey;
  }



  private Statistics<KT> getNextBlockStats()throws IOException {
    if (isNextBlockAvailable()) {
      return incomingStats.get(nextBlockIndex);
    }
    throw new IOException("Invalid call to getNextBlockStats. NextIndex:" + nextBlockIndex + " BlockCount:"+ incomingBlocks.size());
  }

}

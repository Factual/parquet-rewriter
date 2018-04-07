package com.factual.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.codec.CodecConfig;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.*;

import static org.apache.parquet.hadoop.ParquetOutputFormat.*;


/**
 *
 * This is mostly an internal helper that takes a row group worth of data
 * and mutates it.
 *
 * Created by rana
 *
 */
public class ParquetBlockMutator<T,KT extends Comparable<KT>> implements Closeable {

  ReadSupport<? extends T> readSupport;
  WriteSupport<? super T> writeSupport;
  ParquetFileWriter writer;
  FileMetaData sourceFileMetadata;
  RecordWriter<? super T> recordWriter;
  ParquetRewriter.KeyAccessor<? super T,KT> keyAccessor;
  PageReadStore pageReadStore;
  RecordReader<? extends T> recordReader;
  KT lastKey;
  T lastRecord;
  long blockSize;
  long rowGroupSize;
  long remainingRecordCount;
  public long totalReadTime = 0L;
  public long totalWriteTime = 0L;
  public long recordWriterRecordConsumerFlushTime = 0L;
  public long recordWriterColumnIOFlushTime = 0L;
  public long recordWriterFileIOFlushTime = 0L;
  public long pageWriteTime = 0L;
  public long dictionaryWriteTime = 0L;

  static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
    Map<K, Set<V>> setMultiMap = new HashMap<>();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      Set<V> set = new HashSet<>();
      set.add(entry.getValue());
      setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
    }
    return Collections.unmodifiableMap(setMultiMap);
  }

  static final float DEFAULT_MEMORY_POOL_RATIO = 0.95f;
  static final long  DEFAULT_MIN_MEMORY_ALLOCATION = 1 * 1024 * 1024; // 1MB

  public ParquetBlockMutator(
          Configuration conf,
          long rowGroupSize,
          long blockSize,
          PageReadStore readStore,
          FileMetaData fileMetadata,
          ReadSupport<? extends T> readSupport,
          WriteSupport<? super T> writeSupport,
          ParquetFileWriter fileWriter,
          ParquetRewriter.KeyAccessor<? super T,KT> keyAccessor)throws IOException  {

    this.readSupport = readSupport;
    this.writeSupport = writeSupport;
    this.sourceFileMetadata = fileMetadata;
    this.writer = fileWriter;
    this.keyAccessor = keyAccessor;
    this.pageReadStore = readStore;
    this.remainingRecordCount = 0L;
    this.blockSize = blockSize;
    this.rowGroupSize = rowGroupSize;

    if (pageReadStore != null) {
      // initialize a parquet record reader for this page ...
      ReadSupport.ReadContext context = readSupport.init(new InitContext(conf, toSetMultiMap(fileMetadata.getKeyValueMetaData()), sourceFileMetadata.getSchema()));
      RecordMaterializer<? extends T> recordConverter = readSupport.prepareForRead(conf, fileMetadata.getKeyValueMetaData(), sourceFileMetadata.getSchema(), context);
      ColumnIOFactory ioFactory = new ColumnIOFactory(false);
      MessageColumnIO columnIO = ioFactory.getColumnIO(sourceFileMetadata.getSchema());
      this.recordReader = columnIO.getRecordReader(pageReadStore, recordConverter);
      this.remainingRecordCount = readStore.getRowCount();
    }


    // and initialize new record writer
    WriteSupport.WriteContext init = writeSupport.init(conf);


    ParquetProperties props = ParquetProperties.builder()
            .withPageSize(getPageSize(conf))
            .withDictionaryPageSize(getDictionaryPageSize(conf))
            .withDictionaryEncoding(getEnableDictionary(conf))
            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
            .estimateRowCountForPageSizeCheck(getEstimatePageSizeCheck(conf))
            .withMinRowCountForPageSizeCheck(getMinRowCountForPageSizeCheck(conf))
            .withMaxRowCountForPageSizeCheck(getMaxRowCountForPageSizeCheck(conf) * 10)
            .build();


    recordWriter = new RecordWriter<T>(
            conf,
            fileWriter,
            writeSupport,
            init.getSchema(),
            init.getExtraMetaData(),
            rowGroupSize,
            blockSize,
            CodecConfig.getParquetCompressionCodec(conf),
            true,
            props);
  }

  public T readRecord(KT targetKey) throws IOException {
    if (lastKey != null) {
      int compareResult = targetKey.compareTo(lastKey);
      // key is before next queued key - return null
      if (compareResult < 0) {
        return null;
      }
      // key is same as next queued key - return last record
      else if (compareResult == 0){
        lastKey = null;
        T recordOut = lastRecord;
        lastRecord = null;
        return recordOut;
      }
      // key is greater than queued key, writeback queued key
      else {
        writebackLastRecord();
      }
    }

    // if we reach here, we need to seek into remaining records to find a match ...
    while (remainingRecordCount-- > 0) {
      long readTimeStart = System.nanoTime();
      T nextRecord = recordReader.read();
      totalReadTime += (System.nanoTime() - readTimeStart);
      // read should not return null
      if (nextRecord == null) {
        throw new EOFException("Premature EOF detected");
      }
      else {
        // get key from record ...
        KT recordKey = keyAccessor.getKeyForObj(nextRecord);

        int compareResult
                = targetKey.compareTo(recordKey);

        // if we found target key, we are done ...
        if (compareResult == 0) {
          return nextRecord;
        }
        else {
          // otherwise, store current record as last record ...
          lastRecord = nextRecord;
          lastKey = recordKey;
          // if target key is less than current record, return null
          if (compareResult < 0) {
            return null;
          }
          // otherwise, if target key > current record, write back this record
          else {
            writebackLastRecord();
          }
        }
      }
    }
    // we ran out of records to consume... just return null
    return null;
  }

  private void writebackLastRecord() throws IOException {
    if (lastRecord != null) {
      try {
        long nanoTimeStart = System.nanoTime();
        recordWriter.write(lastRecord);
        totalWriteTime += (System.nanoTime() - nanoTimeStart);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      lastRecord = null;
      lastKey = null;
    }
  }

  public void appendRecord(T record) throws IOException {
    readRecord(keyAccessor.getKeyForObj(record));
    try {
      long nanoTimeStart = System.nanoTime();
      recordWriter.write(record);
      totalWriteTime += (System.nanoTime()-nanoTimeStart);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public void skipRecord(T record) throws IOException {
    readRecord(keyAccessor.getKeyForObj(record));
  }


  @Override
  public void close() throws IOException {
    try {
      writebackLastRecord();
      while (remainingRecordCount > 0) {
        long nanoTimeStart = System.nanoTime();
        T nextRecord = recordReader.read();
        totalReadTime += (System.nanoTime() - nanoTimeStart);
        nanoTimeStart = System.nanoTime();
        recordWriter.write(nextRecord);
        totalWriteTime += (System.nanoTime() - nanoTimeStart);
        remainingRecordCount--;
      }

      long nanoTimeStart = System.nanoTime();
      recordWriter.close();

      recordWriterRecordConsumerFlushTime += recordWriter.recordConsumerFlushTime;
      recordWriterColumnIOFlushTime += recordWriter.columnIOFlushTime;
      recordWriterFileIOFlushTime += recordWriter.fileIOFlushTime;
      pageWriteTime += recordWriter.pageWriteTime;
      dictionaryWriteTime += recordWriter.dictionaryWriteTime;

      totalWriteTime += (System.nanoTime() - nanoTimeStart);

    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}

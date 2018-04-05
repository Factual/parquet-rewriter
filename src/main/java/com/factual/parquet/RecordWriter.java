package com.factual.parquet;


import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static org.apache.parquet.Log.DEBUG;
import static org.apache.parquet.Preconditions.checkNotNull;

/**
 *
 * Unfortunately, parquet-mr hides a lot of useful stuff as package private,
 * so until this can be remedied, we need to implement our own record writer
 * and, in turn, entertain a certain amount of hijinks to gain access to
 * some of these classes :-( Also, we want to support both version 1.8.x because
 * this is what Spark / Hadoop(should) currently support, and 1.9.x. Unfortunately,
 * some of the interfaces have changed between the versions :-( In the long term,
 * one hopes that parquet-mr can be convinced to break this habit of the overzealous
 * use of package private and realize that the use-cases they optimize for in their
 * public interfaces might not be the only valid ones. A lot of their implementation
 * details contain useful code that it would be really stupid for other people to
 * reimplement / duplicate.
 *
 * Created by rana
 *
 */
public class RecordWriter<T> {
  private static final Logger LOG = Logger.getLogger(RecordWriter.class);

  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
  private static final int MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;

  private final ParquetFileWriter parquetFileWriter;
  private final WriteSupport<T> writeSupport;
  private final MessageType schema;
  private final long rowGroupSize;
  private long rowGroupSizeThreshold;
  private long nextRowGroupSize;
  private CompressionCodecName codecName;
  private final boolean validating;
  private final Configuration conf;
  private final ParquetProperties props;
  private long recordCount = 0;
  private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
  private long lastRowGroupEndPos = 0;

  private ColumnWriteStore columnStore;
  private PageWriteStore pageStore;
  private RecordConsumer recordConsumer;

  // reflection stuff ...
  private static Class  codecFactoryClass;
  private static Constructor factoryConstructor;
  private static Method flushToFileWriterMethod;
  private static Method getCompressor;
  private static boolean use_1_8_x_Signature = false;
  private static Constructor<PageWriteStore> pageWriteStoreConstructor;
  private static Class heapByteBufferAllocatorClass;
  public long recordConsumerFlushTime = 0L;
  public long columnIOFlushTime = 0L;
  public long fileIOFlushTime = 0L;
  public long pageWriteTime=0L;
  public long dictionaryWriteTime=0L;


  static {
    try {
      codecFactoryClass = Class.forName("org.apache.parquet.hadoop.CodecFactory");
      // we need these for page write store instantiation
      Class chunkPageWriteStoreClaz = Class.forName("org.apache.parquet.hadoop.ColumnChunkPageWriteStore");
      Class codecFactoryByteCompressorClaz = Class.forName("org.apache.parquet.hadoop.CodecFactory$BytesCompressor");


      if (codecFactoryClass != null) {
        try {
          // check for 1.8.x fingerprint
          factoryConstructor = codecFactoryClass.getConstructor(Configuration.class);
          factoryConstructor.setAccessible(true);
          use_1_8_x_Signature = true;
        } catch (NoSuchMethodException e) {
          factoryConstructor = codecFactoryClass.getConstructor(Configuration.class, int.class);
          factoryConstructor.setAccessible(true);
          use_1_8_x_Signature = false;
        }


        if (use_1_8_x_Signature) {
          getCompressor = codecFactoryClass.getMethod("getCompressor", CompressionCodecName.class, int.class);
          getCompressor.setAccessible(true);
          pageWriteStoreConstructor = chunkPageWriteStoreClaz.getConstructor(codecFactoryByteCompressorClaz, MessageType.class);
          pageWriteStoreConstructor.setAccessible(true);

        } else {
          getCompressor = codecFactoryClass.getMethod("getCompressor", CompressionCodecName.class);
          getCompressor.setAccessible(true);
          Class bufferAllocatorClass = Class.forName("org.apache.parquet.bytes.ByteBufferAllocator");
          pageWriteStoreConstructor = chunkPageWriteStoreClaz.getConstructor(codecFactoryByteCompressorClaz, MessageType.class,bufferAllocatorClass);
          pageWriteStoreConstructor.setAccessible(true);
          heapByteBufferAllocatorClass = Class.forName("org.apache.parquet.bytes.HeapByteBufferAllocator");
        }

        flushToFileWriterMethod = chunkPageWriteStoreClaz.getMethod("flushToFileWriter", ParquetFileWriter.class);
        flushToFileWriterMethod.setAccessible(true);
      }
    }
    catch (Exception e) {
      LOG.error(Throwables.getStackTraceAsString(e));
    }
  }




  static PageWriteStore createPageWriteStore(Object compressor,MessageType schema) throws IOException {
    try {
      if (use_1_8_x_Signature) {
        return pageWriteStoreConstructor.newInstance(compressor, schema);
      } else {
        return pageWriteStoreConstructor.newInstance(compressor, schema, heapByteBufferAllocatorClass.newInstance());
      }
    } catch (Exception e) {
      LOG.error(Throwables.getStackTraceAsString(e));
      throw new IOException(e);
    }
  }

  static Object getCompressor(Configuration conf,ParquetProperties props,CompressionCodecName codecName) throws IOException {
    try {
      if (factoryConstructor != null && getCompressor != null) {
        if (use_1_8_x_Signature) {
          return getCompressor.invoke(factoryConstructor.newInstance(conf), codecName, props.getPageSizeThreshold());
        } else {
          return getCompressor.invoke(factoryConstructor.newInstance(conf, props.getPageSizeThreshold()), codecName);
        }
      }
      else {
        throw new IOException("Unable to instantiate compressor object! Static Initialization Failed!");
      }
    }
    catch (Exception e) {
      LOG.error(Throwables.getStackTraceAsString(e));
      throw new IOException(e);
    }
  }

  /**
   * @param conf Hadoop Configuration object
   * @param parquetFileWriter the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param rowGroupSize the size of a block in the file (this will be approximate)
   * @param compressorName the codec used to compress
   * @param validating validate schema
   * @param props Parquet Properties object
   *
   */
  public RecordWriter(
          Configuration conf,
          ParquetFileWriter parquetFileWriter,
          WriteSupport<T> writeSupport,
          MessageType schema,
          Map<String, String> extraMetaData,
          long rowGroupSize,
          long blockSize,
          CompressionCodecName compressorName,
          boolean validating,
          ParquetProperties props) throws IOException {
    this.parquetFileWriter = parquetFileWriter;
    this.writeSupport = checkNotNull(writeSupport, "writeSupport");
    this.schema = schema;
    this.rowGroupSize = rowGroupSize;
    this.rowGroupSizeThreshold = blockSize;
    this.nextRowGroupSize = rowGroupSizeThreshold;
    this.codecName = compressorName;
    this.validating = validating;
    this.conf = conf;
    this.props = props;
    initStore();
  }


  class PageWriteStoreWrapper implements PageWriteStore {

    @Override
    public PageWriter getPageWriter(ColumnDescriptor columnDescriptor) {
      final PageWriter writer = pageStore.getPageWriter(columnDescriptor);
      return new PageWriter() {
        @Override
        public void writePage(BytesInput bytesInput, int i, Statistics<?> statistics, Encoding encoding, Encoding encoding1, Encoding encoding2) throws IOException {
          long nanoStart = System.nanoTime();
          writer.writePage(bytesInput,i,statistics,encoding,encoding1,encoding2);
          pageWriteTime += (System.nanoTime()-nanoStart);
        }

        @Override
        public void writePageV2(int i, int i1, int i2, BytesInput bytesInput, BytesInput bytesInput1, Encoding encoding, BytesInput bytesInput2, Statistics<?> statistics) throws IOException {
          long nanoStart = System.nanoTime();
          writer.writePageV2(i,i1,i2,bytesInput,bytesInput1,encoding,bytesInput2,statistics);
          pageWriteTime += (System.nanoTime()-nanoStart);
        }

        @Override
        public long getMemSize() {
          return writer.getMemSize();
        }

        @Override
        public long allocatedSize() {
          return writer.allocatedSize();
        }

        @Override
        public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
          long nanoStart = System.nanoTime();
          writer.writeDictionaryPage(dictionaryPage);
          dictionaryWriteTime += (System.nanoTime()-nanoStart);
        }

        @Override
        public String memUsageString(String s) {
          return writer.memUsageString(s);
        }
      };
    }
  }

  private void initStore()throws IOException {
    pageStore = createPageWriteStore(getCompressor(conf,props,codecName),schema);
    columnStore = props.newColumnWriteStore(schema, new PageWriteStoreWrapper());
    MessageColumnIO columnIO = new ColumnIOFactory(validating).getColumnIO(schema);
    this.recordConsumer = columnIO.getRecordWriter(columnStore);
    writeSupport.prepareForWrite(recordConsumer);
  }

  public void close() throws IOException, InterruptedException {
    flushRowGroupToStore();
  }

  public void write(T value) throws IOException, InterruptedException {
    writeSupport.write(value);
    ++ recordCount;

    if (recordCount >= this.rowGroupSize) {
      flushRowGroupToStore();
      initStore();
    }
    else {
      checkBlockSizeReached();
    }
  }

  /**
   * @return the total size of data written to the file and buffered in memory
   */
  public long getDataSize() {
    return lastRowGroupEndPos + columnStore.getBufferedSize();
  }

  private void checkBlockSizeReached() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) { // checking the memory size is relatively expensive, so let's not do it for every record.
      long memSize = columnStore.getBufferedSize();
      long recordSize = memSize / recordCount;
      // flush the row group if it is within ~2 records of the limit
      // it is much better to be slightly under size than to be over at all
      /*
      if (memSize > (nextRowGroupSize - 2 * recordSize)) {
        LOG.info(format("mem size %,d > %,d: flushing %,d records to disk.", memSize, nextRowGroupSize, recordCount));
        flushRowGroupToStore();
        initStore();
        recordCountForNextMemCheck = min(max(MINIMUM_RECORD_COUNT_FOR_CHECK, recordCount / 2), MAXIMUM_RECORD_COUNT_FOR_CHECK);
        this.lastRowGroupEndPos = parquetFileWriter.getPos();
      } else {
      */
        recordCountForNextMemCheck = min(
                max(MINIMUM_RECORD_COUNT_FOR_CHECK, (recordCount + (long)(nextRowGroupSize / ((float)recordSize))) / 2), // will check halfway
                recordCount + MAXIMUM_RECORD_COUNT_FOR_CHECK // will not look more than max records ahead
        );
        if (DEBUG) {
          LOG.debug(format("Checked mem at %,d will check again at: %,d ", recordCount, recordCountForNextMemCheck));
/*
        }
*/
      }
    }
  }

  private void flushRowGroupToStore()
          throws IOException {
    long nanoStart = System.nanoTime();
    recordConsumer.flush();
    recordConsumerFlushTime += (System.nanoTime()-nanoStart);

    LOG.info(format("Flushing mem columnStore to file. allocated memory: %,d", columnStore.getAllocatedSize()));
    if (columnStore.getAllocatedSize() > (3 * rowGroupSizeThreshold)) {
      LOG.warn("Too much memory used: " + columnStore.memUsageString());
    }

    if (recordCount > 0) {
      nanoStart = System.nanoTime();
      parquetFileWriter.startBlock(recordCount);
      columnStore.flush();
      columnIOFlushTime += (System.nanoTime()-nanoStart);

      try {
        nanoStart = System.nanoTime();
        flushToFileWriterMethod.invoke(pageStore,parquetFileWriter);
        fileIOFlushTime += (System.nanoTime() - nanoStart);
      }
      catch (Exception e) {
        throw new IOException(e);
      }
      // pageStore.flushToFileWriter(parquetFileWriter);
      recordCount = 0;
      parquetFileWriter.endBlock();
      this.nextRowGroupSize = Math.min(
              parquetFileWriter.getNextRowGroupSize(),
              rowGroupSizeThreshold);
    }

    columnStore = null;
    pageStore = null;
  }
}

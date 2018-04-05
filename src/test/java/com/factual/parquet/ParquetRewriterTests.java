package com.factual.parquet;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

/**
 *
 * Some basic tests to make sure things are working. More high level tests are needed to
 * verify proper splitting behavior etc.
 *
 * Created by rana
 */



public class ParquetRewriterTests {

  static void enforceEmptyDir(Configuration conf, Path path) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        throw new IOException("can not delete path " + path);
      }
    }
    if (!fs.mkdirs(path)) {
      throw new IOException("can not create path " + path);
    }
  }


  static MessageType testSchema = parseMessageType(
            "message test { "
                    + "required binary key; "
                    + "required binary value; "
                    + "} ");

  static Group createTestObject(int key) {
    // some random data ...
    byte[] value = new byte[20];
    new Random().nextBytes(value);
    return createTestObject(key, value);
  }


  static Group createTestObject(int key,byte[] value) {
    SimpleGroupFactory f = new SimpleGroupFactory(testSchema);
    return f.newGroup()
            .append("key",Integer.toString(key))
            .append("value", Binary.fromConstantByteArray(value));
  }

  // sort keys according to the rules of the binary comparator
  static class KeyComparator implements Comparator<Integer> {

    @Override
    public int compare(Integer o1, Integer o2) {
      Binary o1b = Binary.fromConstantByteArray(Integer.toString(o1).getBytes());
      Binary o2b = Binary.fromConstantByteArray(Integer.toString(o2).getBytes());
      return o1b.compareTo(o2b);
    }
  }

  static Set<Integer> getLexographicallySortedKeySet(int startInclusive,int endInclusive) {
    Set<Integer> setOut = Sets.newTreeSet(new KeyComparator());
    IntStream.range(startInclusive,endInclusive).forEach(i -> setOut.add(i));
    return setOut;
  }

  static List<Group> keySetToObjects(Set<Integer> idSet) {
    List<Group> objectList = Lists.newArrayList();

    for (int key : idSet) {
      objectList.add(createTestObject(key));
    }
    return objectList;
  }


  static Path createRootDir(String testName)throws Exception {
    Path root = new Path("target/tests/" + testName+"/");
    enforceEmptyDir(new Configuration(), root);
    return root;
  }

  static Path testFileName(Path root,int id)throws Exception {
    return new Path(root, "test-" + id);
  }

  static Configuration createConf()throws IOException {
    return new Configuration();
  }

  static ParquetWriter<Group> createWriter(Configuration conf,Path fileName, int pageSize,int blockSize)throws Exception {
    return new ParquetWriter<Group>(
            fileName,
            new GroupWriteSupport(),
            UNCOMPRESSED, blockSize, pageSize, 512, true, false, ParquetProperties.WriterVersion.PARQUET_1_0, conf);
  }

  static class Mutation implements  Comparable<Mutation> {
    @Override
    public int compareTo(Mutation o) {
      return Integer.compare(key,o.key);
    }

    enum MutationType {
      UPSERT,
      DELETE
    }
    MutationType mutationType;
    int key;
    byte[] optionalData;

    Mutation(MutationType mutationType, int key) {
      this.mutationType = mutationType;
      this.key = key;

    }
    Mutation(MutationType mutationType, int key, byte[] data) {
      this.mutationType = mutationType;
      this.key = key;
      this.optionalData = data;
    }

    Binary toKeyBytes() { return Binary.fromConstantByteArray(Integer.toString(key).getBytes()); }

    Group toGroupObject() {
      return createTestObject(key,this.optionalData);
    }

    static Mutation createDelete(int key) { return new Mutation(MutationType.DELETE,key);}
    static Mutation createUpsert(int key, byte[] value) {

      return new Mutation(MutationType.UPSERT,key,value);
    }

    static class Builder {
      Random random = new Random();

      Set<Mutation> mutationSet = Sets.newTreeSet(new Comparator<Mutation>() {
        KeyComparator comparator = new KeyComparator();
        @Override
        public int compare(Mutation o1, Mutation o2) {
          return comparator.compare(o1.key,o2.key);
        }
      });

      Builder addDelete(int key) { mutationSet.add(Mutation.createDelete(key)); return this; }
      Builder addUpsert(int key) {
        byte data[] = new byte[20];
        random.nextBytes(data);
        mutationSet.add(Mutation.createUpsert(key,data));
        return this;
      }

      Builder addUpsert(int key,byte [] value) { mutationSet.add(Mutation.createUpsert(key,value)); return this; }

      Set<Mutation> build() {
        return mutationSet;
      }
    }
  }


  static void writeTestFile(Configuration conf,Path fileName,List<Group> objects,int incomingRowGroupSize) throws Exception {
    int rowGroupSize = (incomingRowGroupSize != -1)? incomingRowGroupSize : 100;

    ParquetWriter<Group> writer = createWriter(conf,fileName,rowGroupSize,1024);
    for (Group obj : objects) {
      writer.write(obj);
    }
    writer.close();
  }

  static void applyMutations(Configuration conf,Path sourceFile,Path destFile,Set<Mutation> mutations, int rowGroupSize) throws Exception {
    ParquetRewriter.KeyAccessor<Group,Binary>  keyAccessor = (groupObj) -> { return Binary.fromConstantByteArray(groupObj.getString("key",0).getBytes()); };

    ParquetRewriter<Group,Binary> rewriter = new ParquetRewriter<Group,Binary>(conf,sourceFile,destFile,new GroupReadSupport(),new GroupWriteSupport(),rowGroupSize,keyAccessor,ColumnPath.get("key"));

    for (Mutation m : mutations) {
      if (m.mutationType == Mutation.MutationType.DELETE) {
        rewriter.deleteRecordByKey(m.toKeyBytes());
      }
      else {
        rewriter.appendRecord(m.toGroupObject());
      }
    }
    rewriter.close();
  }

  static void validateMutations(Configuration conf,Path mutatedFile,Set<Integer> originalIdSet,Set<Mutation> mutations) throws Exception {
    Set<Integer> deletedIds = mutations.stream().filter(m -> m.mutationType == Mutation.MutationType.DELETE).map(m -> m.key).collect(Collectors.toCollection(TreeSet::new));
    Map<Integer,Mutation> updatedIds = mutations.stream().filter(m -> m.mutationType == Mutation.MutationType.UPSERT).collect(Collectors.toMap(m ->m.key,m -> m));

    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), mutatedFile).withConf(conf).build();

    Set<Integer> originalIds = Sets.newTreeSet(originalIdSet);

    Group nextRecord = null;
    while ((nextRecord = reader.read()) != null) {
      int  objKey = Integer.parseInt(nextRecord.getString("key",0));
      // remove from original id set ...
      originalIds.remove(objKey);
      // shouldn't find deleted record in current file ...
      assertTrue(!deletedIds.contains(objKey));
      if (updatedIds.containsKey(objKey)) {
        byte[] fileValue = nextRecord.getBinary("value",0).getBytes();
        assertEquals(updatedIds.get(objKey).optionalData,fileValue);
        updatedIds.remove(objKey);
      }
    }
    // remove delete ids from original set
    originalIds.removeAll(deletedIds);
    // we should have encountered all original ids that were not deleted.
    // original ids should be zero now
    assertTrue(originalIds.size() == 0);
    // we also should have encountered all ids in new file. so this set should
    // be zero too
    assertTrue(updatedIds.size() == 0);
  }

  static void mutateAndVerify(Configuration conf,Path rootDir,Set<Integer> originalIdSet, Set<Mutation> mutations, int rowGroupSize)throws Exception {
    Path sourceFile = new Path(rootDir,UUID.randomUUID().toString());
    Path mutatedFile = new Path(rootDir,UUID.randomUUID().toString());

    // write original ...
    writeTestFile(conf,sourceFile,keySetToObjects(originalIdSet),rowGroupSize);
    // apply mutations ...
    applyMutations(conf,sourceFile,mutatedFile,mutations,rowGroupSize);
    // validate mutations
    validateMutations(conf,mutatedFile,originalIdSet,mutations);
  }


  void runTest(String testName,Set<Integer> sortedIds, Set<Mutation> mutations, int rowGroupSize) throws Exception {
    System.out.println("Running Test:" + testName);
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(testSchema, conf);
    Path root = new Path("target/tests/"+testName);
    enforceEmptyDir(conf, root);
    mutateAndVerify(conf,root,sortedIds,mutations,rowGroupSize);

  }
  void runTest(String testName,Set<Integer> sortedIds, Set<Mutation> mutations) throws Exception {
    runTest(testName,sortedIds,mutations,-1);
  }


  @Test
  public void deletionTest() throws Exception {
    Set<Integer> ids = getLexographicallySortedKeySet(1,999);
    Set<Mutation> mutations = new Mutation.Builder()
            .addDelete(1)
            .addDelete(50)
            .addDelete(999).build();

    runTest("basicDelete",ids,mutations);
  }


  @Test
  public void insertTest() throws Exception {
    Set<Integer> ids = getLexographicallySortedKeySet(100,999);
    Set<Mutation> mutations = new Mutation.Builder()
            .addUpsert(0)
            .addUpsert(1)
            .addUpsert(10000)
            .addUpsert(5000)
            .addUpsert(9999).build();

    runTest("basicInsert",ids,mutations);
  }

  @Test
  public void comboTest() throws Exception {
    Set<Integer> ids = getLexographicallySortedKeySet(100,999);
    Set<Mutation> mutations = new Mutation.Builder()
            .addUpsert(0)
            .addUpsert(1)
            .addDelete(100)
            .addDelete(200)
            .addDelete(300)
            .addUpsert(400)
            .addDelete(500)
            .addUpsert(501)
            .addUpsert(502)
            .addUpsert(10000)
            .addUpsert(5000)
            .addUpsert(9999).build();

    runTest("combo",ids,mutations,64);
  }

  @Test
  public void noChangesTest() throws Exception {
    Set<Integer> ids = getLexographicallySortedKeySet(100,999);
    Set<Mutation> mutations = new Mutation.Builder().build();
    runTest("nochange",ids,mutations,64);
  }

  @Test
  public void largeSet() throws Exception {
    Set<Integer> ids = getLexographicallySortedKeySet(100,99999);
    Mutation.Builder builder = new Mutation.Builder();
    for (int i=0;i<99999;++i) {
      if (i % 2 == 0)
        builder.addDelete(i);
      else
        builder.addUpsert(i);

    }

    Set<Mutation> mutations = builder.build();


    runTest("large",ids,mutations,100);
  }

  /*
  public void foo() throws Exception {

    byte[] _99 = "99".getBytes();
    byte[] _100 = "100".getBytes();

    byte[] _200 = "200".getBytes();
    byte[] _2000 = "2000".getBytes();

    System.out.println("Compare 99 to 100:" + WritableComparator.compareBytes(_99,0,_99.length,_100,0,_100.length));
    System.out.println("Compare 200 to 2000:" + WritableComparator.compareBytes(_200,0,_200.length,_2000,0,_2000.length));

    Configuration conf = new Configuration();
    Path root = new Path("target/tests/TestParquetWriter/");
    enforceEmptyDir(conf, root);
    MessageType schema = parseMessageType(
            "message test { "
                    + "required binary key; "
                    + "required binary binary_field; "
                    + "required int32 int32_field; "
                    + "required int64 int64_field; "
                    + "required boolean boolean_field; "
                    + "required float float_field; "
                    + "required double double_field; "
                    + "required fixed_len_byte_array(3) flba_field; "
                    + "required int96 int96_field; "
                    + "} ");
    GroupWriteSupport.setSchema(schema, conf);


    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    Map<String, Encoding> expected = new HashMap<String, Encoding>();
    expected.put("10-" + PARQUET_1_0, PLAIN_DICTIONARY);
    expected.put("1000-" + PARQUET_1_0, PLAIN);
    expected.put("10-" + PARQUET_2_0, RLE_DICTIONARY);
    expected.put("1000-" + PARQUET_2_0, DELTA_BYTE_ARRAY);

    for (int modulo : asList(10, 1000)) {
      System.out.println("Iteration Number:" + modulo);
      for (ParquetProperties.WriterVersion version : ParquetProperties.WriterVersion.values()) {
        Path file = new Path(root, version.name() + "_" + modulo);
        {
          ParquetWriter<Group> writer = new ParquetWriter<Group>(
                  file,
                  new GroupWriteSupport(),
                  UNCOMPRESSED, 1024, 100, 512, true, false, version, conf);

          for (int i = 0; i < 1000; i++) {
            writer.write(
                    f.newGroup()
                            .append("key", Integer.toString(i))
                            .append("binary_field", "test" + (i % modulo))
                            .append("int32_field", 32)
                            .append("int64_field", 64l)
                            .append("boolean_field", true)
                            .append("float_field", 1.0f)
                            .append("double_field", 2.0d)
                            .append("flba_field", "foo")
                            .append("int96_field", Binary.fromConstantByteArray(new byte[12])));
          }
          writer.close();
        }

        Path file2 = new Path(root,version.name()+"_"+modulo+"_2");
        Path file3 = new Path(root,version.name()+"_"+modulo+"_3");
        Path file4 = new Path(root,version.name()+"_"+modulo+"_4");
        Path file5 = new Path(root,version.name()+"_"+modulo+"_5");

        ParquetFileWriter writer2
                = new ParquetFileWriter(
                conf,
                schema,
                file2,
                ParquetFileWriter.Mode.CREATE,100,1024*1024);

        writer2.start();


        ParquetFileReader fileReader = new ParquetFileReader(conf,file, ParquetMetadataConverter.NO_FILTER);

        PageReadStore readStore = null;
        while ((readStore = fileReader.readNextRowGroup()) != null) {
          System.out.println("Read RowGroup of Size:" + readStore.getRowCount());
          ParquetBlockMutator<Group> mutator = new ParquetBlockMutator<Group>(conf, 100,8192, readStore,
                  fileReader.getFileMetaData(), new GroupReadSupport(), new GroupWriteSupport(), writer2, groupKeyAccessor);
          mutator.close();
        }
        writer2.end(Maps.newHashMap());

        fileReader.close();

        {
          ParquetReader<Group> reader2 = ParquetReader.builder(new GroupReadSupport(), file2).withConf(conf).build();
          ParquetMetadata metadata = ParquetFileReader.readFooter(conf, file2);
          for (BlockMetaData b : metadata.getBlocks()) {
            System.out.println("Rewritten File Block RowCount:" + b.getRowCount());
          }
          for (int i = 0; i < 1000; i++) {
            Group group = reader2.read();
            //System.out.println("Group Data:" + group);
            assertEquals("test" + (i % modulo), group.getBinary("binary_field", 0).toStringUsingUTF8());
            assertEquals(32, group.getInteger("int32_field", 0));
            assertEquals(64l, group.getLong("int64_field", 0));
            assertEquals(true, group.getBoolean("boolean_field", 0));
            assertEquals(1.0f, group.getFloat("float_field", 0), 0.001);
            assertEquals(2.0d, group.getDouble("double_field", 0), 0.001);
            assertEquals("foo", group.getBinary("flba_field", 0).toStringUsingUTF8());
            assertEquals(Binary.fromConstantByteArray(new byte[12]),
                    group.getInt96("int96_field", 0));
          }
          reader2.close();
        }
        {

          ParquetRewriter<Group> rewriter = new ParquetRewriter<Group>(conf,file2,file3,new GroupReadSupport(),new GroupWriteSupport(),groupKeyAccessor);

          while(rewriter.isNextBlockAvailable()) {
            rewriter.loadAndMutateNextBlock();
            rewriter.flushMutatedBlock();
          }
          rewriter.close();

          ParquetReader<Group> reader3 = ParquetReader.builder(new GroupReadSupport(), file3).withConf(conf).build();
          ParquetMetadata metadata2 = ParquetFileReader.readFooter(conf, file2);
          for (BlockMetaData b : metadata2.getBlocks()) {
            System.out.println("Rewritten File Block RowCount:" + b.getRowCount());
          }
          for (int i = 0; i < 1000; i++) {
            Group group = reader3.read();
            //System.out.println("Group Data:" + group);
            assertEquals("test" + (i % modulo), group.getBinary("binary_field", 0).toStringUsingUTF8());
            assertEquals(32, group.getInteger("int32_field", 0));
            assertEquals(64l, group.getLong("int64_field", 0));
            assertEquals(true, group.getBoolean("boolean_field", 0));
            assertEquals(1.0f, group.getFloat("float_field", 0), 0.001);
            assertEquals(2.0d, group.getDouble("double_field", 0), 0.001);
            assertEquals("foo", group.getBinary("flba_field", 0).toStringUsingUTF8());
            assertEquals(Binary.fromConstantByteArray(new byte[12]),
                    group.getInt96("int96_field", 0));
          }
          reader3.close();
        }

        {

          ParquetWriter<Group> writer = new ParquetWriter<Group>(
                  file4,
                  new GroupWriteSupport(),
                  UNCOMPRESSED, 1024, 100, 512, true, false, version, conf);

          Set<Integer> sortedSet = Sets.newTreeSet(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
              byte[] o1b = Integer.toString(o1).getBytes();
              byte[] o2b = Integer.toString(o2).getBytes();
              return compareTwoByteArrays(o1b,0,o1b.length,o2b,0,o2b.length);
            }
          });
          for (int i = 1; i < 1000; i++) {
            sortedSet.add(i);
          }
          Set<Integer> verificationSet = Sets.newTreeSet(sortedSet);

          for (int i : sortedSet) {
            System.out.println("Writing:" + i);
            writer.write(
                    f.newGroup()
                            .append("key", Integer.toString(i))
                            .append("binary_field", "test" + (i % modulo))
                            .append("int32_field", 32)
                            .append("int64_field", 64l)
                            .append("boolean_field", true)
                            .append("float_field", 1.0f)
                            .append("double_field", 2.0d)
                            .append("flba_field", "foo")
                            .append("int96_field", Binary.fromConstantByteArray(new byte[12])));
          }
          writer.close();


          ParquetRewriter<Group> rewriter = new ParquetRewriter<Group>(conf,file4,file5,new GroupReadSupport(),new GroupWriteSupport(),groupKeyAccessor);

          Group zero = f.newGroup()
                  .append("key", "0")
                  .append("binary_field", "test" + (200 % modulo))
                  .append("int32_field", 32)
                  .append("int64_field", 64l)
                  .append("boolean_field", true)
                  .append("float_field", 1.0f)
                  .append("double_field", 2.0d)
                  .append("flba_field", "foo")
                  .append("int96_field", Binary.fromConstantByteArray(new byte[12]));

          rewriter.appendRecord(zero);
          verificationSet.add(0);


          rewriter.deleteRecord("100".getBytes());
          verificationSet.remove(100);

          Group replacement = f.newGroup()
                  .append("key", "200")
                  .append("binary_field", "test" + (200 % modulo))
                  .append("int32_field", 32)
                  .append("int64_field", 64l)
                  .append("boolean_field", true)
                  .append("float_field", 1.0f)
                  .append("double_field", 2.0d)
                  .append("flba_field", "foo")
                  .append("int96_field", Binary.fromConstantByteArray(new byte[12]));

          rewriter.appendRecord(replacement);

          Group newGroup = f.newGroup()
                  .append("key", "2000")
                  .append("binary_field", "test" + (2000 % modulo))
                  .append("int32_field", 32)
                  .append("int64_field", 64l)
                  .append("boolean_field", true)
                  .append("float_field", 1.0f)
                  .append("double_field", 2.0d)
                  .append("flba_field", "foo")
                  .append("int96_field", Binary.fromConstantByteArray(new byte[12]));


          rewriter.appendRecord(newGroup);
          verificationSet.add(2000);

          rewriter.close();

          ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file5).withConf(conf).build();
          for (int i = 0; i < 1000; i++) {
            Group group = reader.read();
            System.out.println("Key:" + group.getString("key",0));
            int key = Integer.parseInt(group.getString("key",0));
            assertTrue(verificationSet.contains(key));
            verificationSet.remove(key);
          }
          reader.close();
          assertTrue(verificationSet.isEmpty());


        }

      }
    }
  }
  */
}

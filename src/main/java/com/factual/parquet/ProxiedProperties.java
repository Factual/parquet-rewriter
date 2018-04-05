package com.factual.parquet;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * Created by rana on 2/15/18.
 */
public class ProxiedProperties {


  static Class[] constructorArgsToClassArray() {
    return new Class[] {ParquetProperties.WriterVersion.class, int.class, int.class, boolean.class, int.class, int.class, boolean.class};
  }

  static Object[] constructorArgsToObjectArray(ParquetProperties.WriterVersion writerVersion, int pageSize, int dictPageSize, boolean enableDict, int minRowCountForPageSizeCheck, int maxRowCountForPageSizeCheck, boolean estimateNextSizeCheck) {
    return new Object[]{writerVersion,pageSize,dictPageSize,enableDict,minRowCountForPageSizeCheck,maxRowCountForPageSizeCheck,estimateNextSizeCheck};
  }

  public static ParquetProperties proxy(ParquetProperties.WriterVersion writerVersion, int pageSize, int dictPageSize, boolean enableDict, int minRowCountForPageSizeCheck, int maxRowCountForPageSizeCheck, boolean estimateNextSizeCheck) {
    Enhancer enhancer = new Enhancer();
    enhancer.setSuperclass(ParquetProperties.class);
    enhancer.setCallback(new MethodInterceptor() {
      @Override
      public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy)
              throws Throwable {

        if (method.getName().equals("newValuesWriter")) {
          ParquetProperties target = (ParquetProperties)obj;
          ColumnDescriptor path = (ColumnDescriptor) args[0];

          switch (path.getType()) {
            case BOOLEAN:
              return new BooleanPlainValuesWriter();
            case FIXED_LEN_BYTE_ARRAY:

              return new FixedLenByteArrayPlainValuesWriter(path.getTypeLength(), CapacityByteArrayOutputStream.initialSlabSizeHeuristic(64,target.getPageSizeThreshold(),10), target.getPageSizeThreshold());

            case INT64:
              return new FixedLenByteArrayPlainValuesWriter(12, CapacityByteArrayOutputStream.initialSlabSizeHeuristic(64,target.getPageSizeThreshold(),10), target.getPageSizeThreshold());

            default:
              return new PlainValuesWriter(CapacityByteArrayOutputStream.initialSlabSizeHeuristic(64,target.getPageSizeThreshold(),10), target.getPageSizeThreshold());
          }
        }
        else {
          return proxy.invokeSuper(obj, args);
        }
      }
    });
    return (ParquetProperties) enhancer.create(constructorArgsToClassArray(),constructorArgsToObjectArray(writerVersion,pageSize,dictPageSize,enableDict,minRowCountForPageSizeCheck,maxRowCountForPageSizeCheck,estimateNextSizeCheck));
  }
}

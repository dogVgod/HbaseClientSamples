package com.fuck.exmaples.HbaseClientSample;

import com.google.common.base.Charsets;
import com.stumbleupon.async.Callback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class AsyncHBaseQuickStart {

  public static final String DEFAULT_ZK_DIR = "/hbase";

  // HBase Client
  private HBaseClient hBaseClient;
  private String tableName;
  private String columnFamily;
  private String zkQuorum;

  public AsyncHBaseQuickStart(String tableName, String columnFamily, String zkQuorum) {
    this.tableName = tableName;
    this.columnFamily = columnFamily;
    this.zkQuorum = zkQuorum;
  }

  /**
   * Initialize the client and verify if Table and Cf exists
   *
   * @throws Exception
   */
  public void init() throws Exception {
    if(zkQuorum == null || zkQuorum.isEmpty()) {
      // Follow the default path
      Configuration conf = HBaseConfiguration.create();
      zkQuorum = ZKConfig.getZKQuorumServersString(conf);
    }
    hBaseClient = new HBaseClient(zkQuorum, DEFAULT_ZK_DIR, Executors.newCachedThreadPool());

    // Lets ensure that Table and Cf exits
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicBoolean fail = new AtomicBoolean(false);
    hBaseClient.ensureTableFamilyExists(tableName, columnFamily).addCallbacks(
            new Callback<Object, Object>() {
              @Override
              public Object call(Object arg) throws Exception {
                latch.countDown();
                return null;
              }
            },
            new Callback<Object, Object>() {
              @Override
              public Object call(Object arg) throws Exception {
                fail.set(true);
                latch.countDown();
                return null;
              }
            }
    );

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new Exception("Interrupted", e);
    }

    if(fail.get()) {
      throw new Exception("Table or Column Family doesn't exist");
    }
  }

  /**
   * Puts the data in HBase
   *
   * @param data  Data to be inserted in HBase
   */
  public void putData(byte[] rowKey, String data) throws Exception {
    PutRequest putRequest = new PutRequest(tableName.getBytes(Charsets.UTF_8), rowKey,
                                            columnFamily.getBytes(Charsets.UTF_8), "payload".getBytes(Charsets.UTF_8),
                                            data.getBytes(Charsets.UTF_8));
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicBoolean fail = new AtomicBoolean(false);
    hBaseClient.put(putRequest).addCallbacks(
            new Callback<Object, Object>() {
              @Override
              public Object call(Object arg) throws Exception {
                latch.countDown();
                System.out.println("put ok ");
                return null;
              }
            },
            new Callback<Object, Exception>() {
              @Override
              public Object call(Exception arg) throws Exception {
                fail.set(true);
                System.out.println("put false");
                latch.countDown();
                return null;
              }
            }
    );
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new Exception("Interrupted", e);
    }

    if(fail.get()) {
      throw new Exception("put request failed");
    }
  }

  public byte[] getData(byte[] rowKey) throws Exception {
    GetRequest getRequest = new GetRequest(tableName, rowKey);
    ArrayList<KeyValue> kvs = hBaseClient.get(getRequest).join();
    return kvs.get(0).value();
  }


  public static void main(String[] args) throws Exception {
    byte[] rowKey = UUID.randomUUID().toString().getBytes(Charsets.UTF_8);
    AsyncHBaseQuickStart asyncHBaseQuickStart = new AsyncHBaseQuickStart("test", "data", null);
    asyncHBaseQuickStart.init();
    asyncHBaseQuickStart.putData(rowKey, "Sample Data #1");
    System.out.println(new String(asyncHBaseQuickStart.getData(rowKey)));
  }
}

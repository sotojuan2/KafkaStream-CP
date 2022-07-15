package com.example.colour;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;

public class CustomRocksDBConfig implements RocksDBConfigSetter{

    private static final long TOTAL_OFF_HEAP_MEMORY =  5*1048576L;//DataSize.ofMegabytes(150).toBytes();
    private static final long TOTAL_MEMTABLE_MEMORY = (2)*1048576L;//DataSize.ofMegabytes(50).toBytes();
    private static final long INDEX_FILTER_MEMORY = (2)*1048576L;//DataSize.ofMegabytes(50).toBytes();
    private static double RATIO = (double) INDEX_FILTER_MEMORY / (double) TOTAL_OFF_HEAP_MEMORY;
    private static int N_MEMTABLES = 15;
    private static long BLOCK_SIZE = 32L;


    private static org.rocksdb.Cache cache = new org.rocksdb.LRUCache(TOTAL_OFF_HEAP_MEMORY, -1, false, RATIO);
    private static org.rocksdb.WriteBufferManager writeBufferManager = new org.rocksdb.WriteBufferManager(TOTAL_MEMTABLE_MEMORY, cache);

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        Properties config=null;
        try {
            config = loadConfig("/test/rocksdb.txt");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        };
       
        
  
        // TODO Auto-generated method stub
        if(config!=null)
            System.out.println("configuring new Javis rocksdb configuration "+config.getProperty("rocksdb"));
        else{
            System.out.println("errrrooooor ");
        }
        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        System.out.println("El valor de cache es : "+tableConfig.cacheIndexAndFilterBlocks());
        // These three options in combination will limit the memory used by RocksDB to the size passed to the block cach5e (TOTAL_OFF_HEAP_MEMORY)5
        tableConfig.setBlockCache(cache);
        Boolean boolean1 = config.getProperty("CacheIndexAndFilterBlocks")==null ? true:Boolean.parseBoolean(config.getProperty("CacheIndexAndFilterBlocks"));
        tableConfig.setCacheIndexAndFilterBlocks(boolean1);
        options.setWriteBufferManager(writeBufferManager);
        //JSOTO parameters
        System.out.println("EEEEE El valor de indices es : "+tableConfig.cacheIndexAndFilterBlocks());
        options.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);
        String str = config.getProperty("maxOpenFiles")==null ? "-1":config.getProperty("maxOpenFiles");
        System.out.println("el valor maxOpenFiles es : "+config.getProperty(str));
        options.setMaxOpenFiles(Integer.parseInt(str));
       // JSOTO compaction
       options.setMaxCompactionBytes(2*1024L);
        options.setDisableAutoCompactions(true);
        //options.setCompactionStyle(CompactionStyle.LEVEL);

        CompactionStyle compactionStyle = options.compactionStyle();

        System.out.println("Estilo de compactacion "+compactionStyle.toString());

        CompactionOptionsUniversal compOpt=  options.compactionOptionsUniversal();

        

        System.out.println("Valores de compactacion maxCompactionBytes "+options.maxCompactionBytes());
        //System.out.println("Valores de compactacion sizeRatio "+compOpt.sizeRatio());
        //System.out.println("Valores de compactacion maxMergeWidth"+compOpt.maxMergeWidth());

        // These options are recommended to be set when bounding the total memory
        // See #2 below
        tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
        tableConfig.setPinTopLevelIndexAndFilter(true);
        // See #3 below
        tableConfig.setBlockSize(BLOCK_SIZE);
        options.setMaxWriteBufferNumber(N_MEMTABLES);
        options.setWriteBufferSize(1*1048576L);//3145728L
        //JSOTO table

        //options.setMaxBackgroundJobs(10);


        //sylvan
        //options.setUseDirectReads(true);
        //options.setUseDirectIoForFlushAndCompaction(true);
        // Enable compression (optional). Compression can decrease the required storage
        // and increase the CPU usage of the machine. For CompressionType values, see
        // https://javadoc.io/static/org.rocksdb/rocksdbjni/6.4.6/org/rocksdb/CompressionType.html.
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);

        options.setTableFormatConfig(tableConfig);
    }

    @Override
    public void close(String storeName, Options options) {
        // TODO Auto-generated method stub
        
    }


    public static Properties loadConfig( String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
          throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
          cfg.load(inputStream);
        }
        return cfg;
      }
    
}

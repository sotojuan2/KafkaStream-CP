package com.example.colour;

import java.util.Map;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;

public class customRocksDBConfigECI2 implements RocksDBConfigSetter{

    private static final long TOTAL_OFF_HEAP_MEMORY =  150*1048576L;//DataSize.ofMegabytes(150).toBytes();
    private static final long TOTAL_MEMTABLE_MEMORY = (50)*1048576L;//DataSize.ofMegabytes(50).toBytes();
    private static final long INDEX_FILTER_MEMORY = (50)*1048576L;//DataSize.ofMegabytes(50).toBytes();
    private static double RATIO = (double) INDEX_FILTER_MEMORY / (double) TOTAL_OFF_HEAP_MEMORY;
    private static int N_MEMTABLES = 15;
    private static long BLOCK_SIZE = 32L;


    private static org.rocksdb.Cache cache = new org.rocksdb.LRUCache(TOTAL_OFF_HEAP_MEMORY, -1, false, RATIO);
    private static org.rocksdb.WriteBufferManager writeBufferManager = new org.rocksdb.WriteBufferManager(TOTAL_MEMTABLE_MEMORY, cache);

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        // TODO Auto-generated method stub
        System.out.println("configuring new Original rocksdb configuration");
        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        System.out.println("El valor de cache es : "+cache.getPinnedUsage());
        // These three options in combination will limit the memory used by RocksDB to the size passed to the block cache (TOTAL_OFF_HEAP_MEMORY)
        tableConfig.setBlockCache(cache);
        tableConfig.setCacheIndexAndFilterBlocks(true);
        options.setWriteBufferManager(writeBufferManager);
        //JSOTO parameters
        //options.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);
        options.setMaxOpenFiles(15);
        options.setMaxCompactionBytes(20*1024L);
        //options.setDisableAutoCompactions(true);
        options.setCompactionStyle(CompactionStyle.LEVEL);

        // These options are recommended to be set when bounding the total memory
        // See #2 below
        tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
        tableConfig.setPinTopLevelIndexAndFilter(true);
        // See #3 below
        tableConfig.setBlockSize(BLOCK_SIZE);
        options.setMaxWriteBufferNumber(N_MEMTABLES);
        options.setWriteBufferSize(1*1048576L);//3145728L JSOTO TEST
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
    
}
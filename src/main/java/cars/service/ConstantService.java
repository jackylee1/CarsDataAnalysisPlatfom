package cars.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * 常量服务
 */
@Service
public class ConstantService {

    public String getHdfsAddress() {
        return hdfsAddress;
    }

    public String getHdfsRootpath() {
        return hdfsRootpath;
    }

    public String getHadoopUserName() {
        return hadoopUserName;
    }

    public String getWebhdfsAddress() {
        return webhdfsAddress;
    }

    public String getHbaseMaster() {
        return hbaseMaster;
    }

    public String getZookeeperClientport() {
        return zookeeperClientport;
    }

    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    public String getHbaseTableName() {
        return hbaseTableName;
    }

    /**
     * 读取配置文件
     */
    @Value("${cars.hadoop.hdfs.address}") private String hdfsAddress;
    @Value("${cars.hadoop.hdfs.rootpath}") private String hdfsRootpath;
    @Value("${cars.hadoop.username}") private String hadoopUserName;
    @Value("${cars.hadoop.webhdfs.address}") private String webhdfsAddress;
    @Value("${cars.hadoop.hbase.master}") private String hbaseMaster;
    @Value("${cars.hadoop.hbase.zookeeper.property.clientPort}") private String zookeeperClientport;
    @Value("${cars.hadoop.hbase.zookeeper.quorum}") private String zookeeperQuorum;
    @Value("${cars.hadoop.hbase.tableName}") private String hbaseTableName;
}

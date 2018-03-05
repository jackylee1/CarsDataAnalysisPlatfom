package cars.controller;

import cars.service.ConstantService;
import cars.service.HadoopService;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.file.Paths;

/**
 *  Hadoop操作控制器
 */
@Controller
@RequestMapping(value = "/hadoop")
public class HadoopController {

    /**
     * 更新HBase数据库
     * @param root 待更新目录的根路径
     * @return 成功true 异常false
     */
    @ResponseBody
    @RequestMapping(value = "/update_hbase", method = RequestMethod.GET)
    public JSONObject updateHBase(@RequestParam(value = "root", defaultValue = "/") String root) {
        JSONObject jsonObject = new JSONObject();
        try {
            hadoopService.updateHBase(root.replace(constantService.getHdfsRootpath(), ""), getHBaseTable());
        } catch (IOException e) {
            e.printStackTrace();
            jsonObject.put("success", "false");
            return jsonObject;
        }
        jsonObject.put("success", "true");
        return jsonObject;
    }

    /**
     * 列出HDFS父目录下的所有文件
     * @param path  父目录路径
     * @param id    目录文件id
     * @return  成功返回json数组形式的结果 失败返回null
     */
    @ResponseBody
    @RequestMapping(value = "/list_dirs", method = RequestMethod.GET)
    public JSONArray listDirs(@RequestParam(value = "path", defaultValue = "/") String path, @RequestParam(value = "id", defaultValue = "-1") int id) {
        try {
            return hadoopService.listDirsByWebhdfs(path, id);
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * 根据时间搜索视频
     * @param startTime 开始时间
     * @param endTime   结束时间
     * @return  成功返回json数组形式的结果 失败返回null
     */
    @ResponseBody
    @RequestMapping(value = "/search_by_time", method = RequestMethod.GET)
    public JSONArray searchVideosByTime(@RequestParam(value = "startTime", defaultValue = "") String startTime, @RequestParam(value = "endTime", defaultValue = "") String endTime) {
        try {
            return hadoopService.searchVideoByTime(startTime, endTime, getHBaseTable());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 将日志输出到httpResponse流
     * @param request   httpRequest
     * @param response  httpResponse
     */
    @RequestMapping(value = "/log_stream", method = RequestMethod.GET)
    public void getLogStream(HttpServletRequest request, HttpServletResponse response) {
        /*获取视频名与日志路径*/
        String videoName = Paths.get(request.getParameter("fpath")).getFileName().toString();
        String logPath = null;
        try {
            logPath = hadoopService.getLogPath(videoName, getHBaseTable());
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        if(null == logPath) {
            return;
        }

        /*获取日志地址*/
        String logAddress = constantService.getHdfsAddress() + Paths.get(constantService.getHdfsRootpath(), logPath).toString();

        /*从hdfs日志流读取 输出到httpResponse流*/
        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
            reader =  new BufferedReader(new InputStreamReader(getFileSystem().open(new Path(logAddress)))); /*hdfs日志输入流*/

            response.setContentType("application/octet-stream");
            writer =  new BufferedWriter(new OutputStreamWriter(response.getOutputStream())); /*http输出流*/

            /*输出到response流*/
            String line = reader.readLine();
            if(null != line && !line.isEmpty()) {
                writer.write(line);
            }
            while (null != (line = reader.readLine())) {
                if(line.trim().isEmpty()) {
                    continue;
                }
                writer.newLine();
                writer.write(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;

        }
        finally{
            try {
                if(reader != null){
                    reader.close();
                }
                if(writer != null){
                    writer.flush();
                    writer.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 将视频输出到httpResponse流
     * @param request   httpRequest
     * @param response  httpResponse
     */
    @RequestMapping(value = "/video_stream", method = RequestMethod.GET)
    public void videoStream(HttpServletRequest request, HttpServletResponse response) {
        /*获取视频名与视频路径*/
        String videoName = Paths.get(request.getParameter("fpath")).getFileName().toString();

        String videoPath = null;
        try {
            videoPath = hadoopService.getVideoPath(videoName, getHBaseTable());
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        if(null == videoPath) {
            return;
        }

        /*获取日志地址*/
        String videoAddress = constantService.getHdfsAddress() + Paths.get(constantService.getHdfsRootpath(), videoPath).toString();

        /*从hdfs视频流读取 输出到httpResponse流*/
        FSDataInputStream in = null;
        OutputStream out = null;

        try {
            /*获取hdfs文件系统*/
            FileSystem fileSystem = getFileSystem();

            in = fileSystem.open(new Path(videoAddress));

            response.setHeader("Content-type","Video/mp4");
            out = response.getOutputStream();

            final long videoSize = fileSystem.getFileStatus(new Path(videoAddress)).getLen();
            String range = request.getHeader("Range");

            /*根据请求视频段的范围分别处理*/
            if(range == null) {
                response.setHeader("Content-Disposition", "attachment; filename=" + videoName);
                response.setContentType("application/octet-stream");
                response.setContentLength((int)videoSize);
                IOUtils.copyBytes(in, out, videoSize, false);
            }
            else {
                long start = Integer.valueOf(range.substring(range.indexOf("=") + 1, range.indexOf("-")));
                long count = videoSize - start;
                long end;
                if(range.endsWith("-")) {
                    end = videoSize - 1;
                }
                else {
                    end = Integer.valueOf(range.substring(range.indexOf("-") + 1));
                }

                String ContentRange = "bytes " + String.valueOf(start) + "-" + end + "/" + String.valueOf(videoSize);

                response.setStatus(206);
                response.setContentType("Video/mpeg4");
                response.setHeader("Content-Range",ContentRange);
                in.seek(start);
                IOUtils.copyBytes(in, out, count, false);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        finally {
            try {
                if(in != null){
                    in.close();
                }
                if(out != null){
                    out.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /*Hadoop文件系统 线程局部变量*/
    private ThreadLocal<FileSystem> fileSystemThreadLocal = new ThreadLocal<>();
    private FileSystem getFileSystem() throws IOException{
        if(null != fileSystemThreadLocal.get()) {
            return fileSystemThreadLocal.get();
        }
        String filePath = constantService.getHdfsAddress() + constantService.getHdfsRootpath();
        Configuration config = new Configuration();
        FileSystem fileSystem = null;
        fileSystem = FileSystem.get(URI.create(filePath), config);
        fileSystemThreadLocal.set(fileSystem);
        return fileSystem;
    }

    /* HBase数据表 线程局部变量*/
    private ThreadLocal<Table> hbaseTableThreadLocal = new ThreadLocal<>();
    private Table getHBaseTable() throws IOException{
        if(null != hbaseTableThreadLocal.get()) {
            return hbaseTableThreadLocal.get();
        }
        /*获得hbase配置文件对象*/
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", constantService.getZookeeperClientport());
        configuration.set("hbase.zookeeper.quorum", constantService.getZookeeperQuorum());
        configuration.set("hbase.master", constantService.getHbaseMaster());

        /*建立hbase连接、获得hbase回话*/
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        /*确认数据库存在*/
        TableName tableName = TableName.valueOf(constantService.getHbaseTableName());
        if(!admin.tableExists(tableName)) {
            /*创建表结构对象*/
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            /*创建列族结构对象*/
            hTableDescriptor.addFamily(new HColumnDescriptor("path"));
            hTableDescriptor.addFamily(new HColumnDescriptor("time"));
            /*创建表*/
            admin.createTable(hTableDescriptor);
        }
        Table table = connection.getTable(TableName.valueOf(constantService.getHbaseTableName()));

        admin.close();

        hbaseTableThreadLocal.set(table);
        return table;
    }


    @Autowired
    private HadoopService hadoopService;

    @Autowired
    private ConstantService constantService;

    private Logger logger = LoggerFactory.getLogger(HadoopController.class);
}

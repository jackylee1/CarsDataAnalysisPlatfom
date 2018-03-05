package cars.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;


/**
 * Hadoop相关的服务
 */
@Service
public class HadoopService {

    /**
     * 通过时间搜索视频
     * @param startTime  开始时间
     * @param endTime   结束时间
     * @param table     HBase数据表
     * @return  返回json数组形式的结果
     * @throws IOException  可能抛出IOException异常
     */
    public JSONArray searchVideoByTime(String startTime, String endTime, Table table) throws IOException{
        /*将输入时间转为UNIX时间戳 单位为秒*/
        long startDateTime = parseOriginName(startTime);
        long endDateTime = parseOriginName(endTime);

        /*HBase条件查询区间内的结果*/
        FilterList filterList = new FilterList();
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("time"), Bytes.toBytes("seconds"),
                    CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(startDateTime)));
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("time"), Bytes.toBytes("seconds"),
                CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(endDateTime)));

        Scan scan = new Scan();
        scan.setFilter(filterList);
        ResultScanner rs = table.getScanner(scan);

        /*将结果的文件名称与路径写入json*/
        JSONArray jsonArray = new JSONArray();
        for (Result result: rs) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("name", Bytes.toString(result.getRow()));
            jsonObject.put("path", Bytes.toString(CellUtil.cloneValue(result.getColumnCells(Bytes.toBytes("path"), Bytes.toBytes("videoPath")).get(0))));
            jsonArray.add(jsonObject);
        }
        return jsonArray;
    }

    /**
     * 通过webhdfs方式获取hdfs路径下所有文件 即命令curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS&user.name=<NAME>"
     * @param path  父目录路径
     * @param id    目录文件id
     * @return  返回json数组形式的结果
     * @throws IOException  可能抛出IOException异常
     */
    public JSONArray listDirsByWebhdfs(String path, long id) throws IOException {
        /*生成hdfs路径*/
        boolean isRoot = -1L == id;
        path = Paths.get(constantService.getHdfsRootpath(), path).toString();

        /*生成webhdfs请求的url*/
        String command = MessageFormat.format("/webhdfs/v1{0}?op=LISTSTATUS&user.name={1}", path, constantService.getHadoopUserName());
        URL url = new URL(new URL(constantService.getWebhdfsAddress()), command);

        /*设置http连接并读取结果*/
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.connect();
        String resp = getHttpResult(conn);
        conn.disconnect();

        /*处理返回的json结果 获得每个文件的id parentId name isParent path*/
        JSONObject root = JSON.parseObject(resp);
        JSONArray dirArray = root.getJSONObject("FileStatuses").getJSONArray("FileStatus");
        int size = dirArray.size();

        JSONArray dirs = new JSONArray();
        for(int i = 0; i < size; i++) {
            JSONObject dirObject = new JSONObject();

            JSONObject parseObject = dirArray.getJSONObject(i);
            String dirName = parseObject.getString("pathSuffix");
            /*若该目录为日志或日志文件夹 跳过*/
            if(dirName.endsWith("csv") || dirName.contains("Log") || dirName.contains("log")) {
                continue;
            }
            dirObject.put("id", parseObject.getLong("fileId"));
            dirObject.put("name", dirName);
            if(!isRoot) {
                dirObject.put("parentId", id);
            }
            dirObject.put("isParent", String.valueOf(parseObject.getLong("childrenNum") > 0));
            dirObject.put("path", Paths.get(path.replaceFirst(constantService.getHdfsRootpath(), ""), parseObject.getString("pathSuffix")).toString());

            dirs.add(dirObject);
        }
        return dirs;
    }

    /**
     * 查询日志路径
     * @param videoName 视频名称
     * @param table     HBase数据表
     * @return  若成功则返回日志路径 失败则返回null
     * @throws IOException  可能抛出IOException异常
     */
    public String getLogPath(String videoName, Table table) throws IOException {
        /*查询HBase*/
        Get get = new Get(Bytes.toBytes(videoName));
        get.addColumn(Bytes.toBytes("path"), Bytes.toBytes("logPath"));
        Result result = table.get(get);

        /*若查询不到结果 返回null*/
        if(null == result || result.isEmpty()) {
            return null;
        }

        /*若查询结果中不存在日志路径 返回null*/
        List<Cell> cells = result.listCells();
        if(null ==  cells || cells.isEmpty()) {
            return null;
        }

        return Bytes.toString(CellUtil.cloneValue(cells.get(0)));
    }

    /**
     * 查询视频路径
     * @param videoName 视频名称
     * @param table     HBase数据表
     * @return  若成功则返回视频路径 失败则返回null
     * @throws IOException  可能抛出IOException异常
     */
    public String getVideoPath(String videoName, Table table) throws IOException {
        /*查询HBase*/
        Get get = new Get(Bytes.toBytes(videoName));
        get.addColumn(Bytes.toBytes("path"), Bytes.toBytes("videoPath"));
        Result result = table.get(get);

        /*若查询不到结果 返回null*/
        if(null == result || result.isEmpty()) {
            return null;
        }

        /*若查询结果中不存在视频路径 返回null*/
        List<Cell> cells = result.listCells();
        if(null ==  cells || cells.isEmpty()) {
            return null;
        }
        return Bytes.toString(CellUtil.cloneValue(cells.get(0)));
    }

    /**
     * 更新HBase数据库
     * @param root  待更新目录的根路径
     * @param table HBase数据表
     * @throws IOException  可能抛出IOException异常
     */
    public void updateHBase(String root, Table table) throws IOException {
        /*获取该路径下的所有文件 依次处理*/
        JSONArray dirs = listDirsByWebhdfs(root, -1L);
        for(int i = 0; i < dirs.size(); i++) {
            /*获取文件名称与路径*/
            JSONObject dirObject = dirs.getJSONObject(i);
            String fileName = dirObject.getString("name");
            String path = Paths.get(root, fileName).toString();

            if(fileName.endsWith(".csv")) { /*若该文件为日志*/
                /*获取对应视频文件名*/
                String videoName = fileName.replace("csv", "mp4");

                /*更新该日志路径与时间信息*/
                Put put = new Put(Bytes.toBytes(videoName));

                put.addColumn(Bytes.toBytes("path"), Bytes.toBytes("logPath"), Bytes.toBytes(path));

                long dateTime = parseVideoName(videoName);
                put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("seconds"), Bytes.toBytes(dateTime));

                table.put(put);
            }
            else if(fileName.endsWith(".mp4")) {
                /*若该文件为视频 则更新该视频路径与时间信息*/
                Put put = new Put(Bytes.toBytes(fileName));

                put.addColumn(Bytes.toBytes("path"), Bytes.toBytes("videoPath"), Bytes.toBytes(path));

                long dateTime = parseVideoName(fileName);
                put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("seconds"), Bytes.toBytes(dateTime));

                table.put(put);
            }
            else {
                /*若该文件为目录 继续处理该目录下的文件*/
                updateHBase(path, table);
            }
        }
    }

    /**
     * 获取http请求的结果
     * @param conn http连接
     * @return  返回请求结果
     * @throws IOException  可能抛出IOException异常
     */
    private String getHttpResult(HttpURLConnection conn) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();

        /*获取http连接的输入流*/
        InputStream inputStream = conn.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));

        /*读取输入流*/
        String line = null;
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
        }

        /*关闭输入流*/
        reader.close();
        inputStream.close();

        return stringBuilder.toString();
    }

    /**
     * 从视频名称提取UNIX时间戳 单位为秒
     * @param videoName 视频名称
     * @return  成功返回UNIX时间戳 失败返回0
     */
    private long parseVideoName(String videoName) {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss", Locale.CHINA);
            return simpleDateFormat.parse(videoName.replace(".mp4", "")).getTime() / 1000L;
        }
        catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * 将标准时间转为UNIX时间戳 单位为秒
     * @param dateTime  待转化的日期时间
     * @return  成功返回UNIX时间戳 失败返回0
     */
    private long parseOriginName(String dateTime) {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.CHINA);
            return simpleDateFormat.parse(dateTime).getTime() / 1000L;
        }
        catch (ParseException e) {
            e.printStackTrace();
            return 0;
        }
    }

    @Autowired
    private ConstantService constantService;

    private org.slf4j.Logger logger = LoggerFactory.getLogger(HadoopService.class);
}

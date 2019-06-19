package com.waltz.etl;

import com.waltz.etl.base.BaseMR;
import com.waltz.etl.utils.IPUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class TextToAvro extends BaseMR {

    public static Schema schema = null;
    public static Schema.Parser parser = new Schema.Parser();

    private static class TextToavroMapper extends Mapper<LongWritable, Text, AvroKey, NullWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //根据user_install_status.avro文件内的格式，生成指定格式的schema对象
            schema = parser.parse("/nginx_access_log.avse");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String remote_addr, remote_user, time_local, request, status_code, body_bytes_sent, http_referer, user_agent, x_forwarded_for;
            String request_timestamp=null, remote_timezone=null, method=null, http_url=null, http_version=null,region=null;
            String[] splits = value.toString().split("\001");
            if (splits == null || splits.length != 10) {
                System.out.println("");
                System.out.println("_______________________");
                System.out.println("bad line");
                System.out.println(value.toString());
                context.getCounter("etl_error", "bad line").increment(1l);
                return;
            } else {
                /**
                 * 客户端的ip地址，
                 */
                remote_addr = splits[0];
                /**
                 * 客户端用户名称
                 */
                remote_user = splits[1];
                /**
                 * 访问时间和时区
                 */
                time_local = splits[2];
                /**
                 * 请求的方式、请求的url、请求的http协议版本 GET：http请求方式，有GET和POST两种
                 */
                request = splits[3];
                /**
                 * Http请求的状态 服务状态，200表示正常，常见的还有，301永久重定向、4XX表示请求出错、5XX服务器内部错误
                 */
                status_code = splits[4];
                /**
                 * 发送给客户端客户端文件的大小。传送字节数为5209，单位为byte
                 */
                body_bytes_sent = splits[5];
                /**
                 * url跳转来源 refer:即当前页面的上一个网页
                 */
                http_referer = splits[6];
                /**
                 *用户终端浏览器等信息 agent字段：通常用来记录操作系统、浏览器版本、浏览器内核等信息
                 */
                user_agent = splits[7];
                /**
                 *用来表示 HTTP 请求端真实 IP
                 */
                x_forwarded_for = splits[8];
            }
            /**
             * 分解time_local
             */
            if (time_local != null && !time_local.equals("")) {
                try {
                    request_timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH).parse(time_local));
                } catch (ParseException e) {
                    request_timestamp = null;
                    e.printStackTrace();
                }
                try {
                    remote_timezone = time_local.split(" ")[1];
                } catch (Exception e) {
                    remote_timezone = null;
                }
            }
            /**
             * 分解request
             */
            if(remote_addr!=null&&!remote_addr.equals("")){
                region= IPUtil.getAreaByIp(remote_addr);
            }
            if (request != null && !request.equals("")) {
                String[] request_splits = request.split(" ");
                if (request_splits.length == 3) {
                    method = request_splits[0];
                    http_url = request_splits[1];
                    http_version = request_splits[2];
                }else{
                    context.getCounter("etl_error", "bad line").increment(1l);
                    return;
                }
            }
            //根据schema文件，创建Schema对象.
            GenericRecord record = new GenericData.Record(TextToAvro.schema);
            record.put("remote_addr",remote_addr);
            record.put("remote_user",remote_user);
            record.put("request_timestamp",request_timestamp);
            record.put("remote_timezone",remote_timezone);
            record.put("method",method);
            record.put("http_url",http_url);
            record.put("http_version",http_version);
            record.put("status_code",status_code);
            record.put("body_bytes_sent",body_bytes_sent);
            record.put("http_referer",http_referer);
            record.put("user_agent",user_agent);
            record.put("x_forwarded_for",x_forwarded_for);
            record.put("region",region);
            context.getCounter("etl_good","good line num").increment(1l);

            context.write(new AvroKey<>(record), NullWritable.get());
        }
    }

    /**
     * 返回任务的job的对象，由子类去实现
     *
     * @param conf
     * @return TODO(这里描述每个参数, 如果有返回值描述返回值, 如果有异常描述异常)
     */
    @Override
    public Job getJob(Configuration conf) throws IOException {
        //设置reduce输出压缩
        conf.set(FileOutputFormat.COMPRESS,"true");
        conf.set(FileOutputFormat.COMPRESS_CODEC, SnappyCodec.class.getName());
        Job job = Job.getInstance(conf);

        job.setJarByClass(TextToAvro.class);

        job.setMapperClass(TextToavroMapper.class);
        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        AvroJob.setOutputKeySchema(job,schema);

        FileInputFormat.addInputPath(job,getFirstJobInputPath(conf));
        FileOutputFormat.setOutputPath(job,getOutputPath(conf));
        return job;
    }

    /**
     * 返回基础任务名称，由子类去实现
     */
    @Override
    public String getJobName() {
        return "text2avro";
    }
}

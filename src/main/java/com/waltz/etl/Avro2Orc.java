package com.waltz.etl;

import com.waltz.etl.base.BaseMR;
import com.waltz.etl.base.OrcUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Avro2Orc extends BaseMR {
    public static Schema schema = null;
    public static Schema.Parser parse = new Schema.Parser();

    @Override
    public Job getJob(Configuration conf) throws IOException {

        //关闭map的推测执行，使得一个map处理 一个region的数据
        conf.set("mapreduce.map.spedulative", "false");
        //设置orc文件snappy压缩
        conf.set("orc.compress", CompressionKind.SNAPPY.name());
        //设置orc文件 有索引
        conf.set("orc.create.index", "true");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Avro2Orc.class);
        job.setMapperClass(Avro2OrcMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Writable.class);
//        无reduce
        job.setNumReduceTasks(0);
        job.setInputFormatClass(AvroKeyInputFormat.class);
        //根据user_install_status.avro文件内的格式，生成指定格式的schema对象
        schema = parse.parse(Avro2Orc.class.getResourceAsStream("/hainiu.avro"));
        AvroJob.setInputKeySchema(job, schema);
        job.setOutputFormatClass(OrcNewOutputFormat.class);
        FileInputFormat.addInputPath(job, getFirstJobInputPath(conf));
        FileOutputFormat.setOutputPath(job, getOutputPath(conf));
        return job;
    }

    @Override
    public String getJobName() {
        return "etlAvro2Orc_topic10";
    }

    public static class Avro2OrcMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, NullWritable, Writable> {
        OrcUtil orcUtil = new OrcUtil();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            orcUtil.setOrcTypeReadSchema();
            orcUtil.setOrcTypeWriteSchema();
        }

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            //得到一行的对象
            GenericRecord datum = key.datum();
            String uip = (String) datum.get("uip");
            String datetime = (String) datum.get("datetime");
            //String method = (String) datum.get("method");
            //String http = (String) datum.get("http");
            String top1 = (String) datum.get("top");
            String top = "";
            String regex = "/topics/\\d+";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(top1);
            if (matcher.find()) {
                top = matcher.group();
            } else {
                context.getCounter("etl_err", "notopics line num").increment(1L);
                return;
            }
            //orcUtil.addAttr(uip,datetime,method,http,uid,country,status1,status2,usagent);
            orcUtil.addAttr(uip, datetime, top);
            Writable w = orcUtil.serialize();
            context.getCounter("etl_good", "good line num").increment(1L);
            System.out.println(uip + "    " + top);
            context.write(NullWritable.get(), w);
        }
    }
}
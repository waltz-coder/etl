//Text2Avro
package com.waltz.etl;


import com.waltz.etl.base.BaseMR;
import com.waltz.etl.utils.IPUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Text2Avro extends BaseMR
{
    public static Schema schema = null;

    public static Schema.Parser parse = new Schema.Parser();

    public static class Text2AvroMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable>
    {



        @Override
        protected void setup(Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable>.Context context)
                throws IOException, InterruptedException {
            //根据user_install_status.avro文件内的格式，生成指定格式的schema对象
            schema = parse.parse(Text2Avro.class.getResourceAsStream("/hainiu.avro"));

        }
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            String[] splits = line.split("\001");
            if(splits == null || splits.length != 10){
                System.out.println("==============");
                System.out.println(value.toString());
                context.getCounter("etl_err", "bad line num").increment(1L);
                return;
            }

//            System.out.println(util.getIpArea("202.8.77.12"));
            String uip1 = splits[0];
            String uip = IPUtil.getAreaByIp(uip1);

            String datetime = splits[2];
            StringBuilder sb=new StringBuilder(datetime);

            SimpleDateFormat sdf=new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH);
            String sy=sb.toString();
            Date myDate = null;
            try
            {
                myDate = sdf.parse(sy);
            } catch (ParseException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            SimpleDateFormat sdf2=new SimpleDateFormat("yyyyMMddHHmmss");
            //System.out.println(myDate);
            String format = sdf2.format(myDate);
            //GET /categories/8?filter=recent&page=12 HTTP/1.1
            String url1 = splits[3];
            StringBuilder sb2=new StringBuilder(url1);

            String url = sb2.toString();
            String method="";
            String top="";
            String top1="";
            String http="";
            if(url!=null)
            {
                String[] s = url.split(" ");
                if(s.length==3)
                {
                    method=s[0];
                    http=s[2];

                    top1=s[1];
                    if(top1.contains("."))
                    {
                        context.getCounter("etl_err", "no line num").increment(1L);
                        return;
                    }
                    else
                    {
                        top=top1;
                    }
                }
            }

            String status1 = splits[4];
            String status2 = splits[5];
            String post = splits[6];
            String from = splits[7];
            String usagent1 = splits[8];
            StringBuilder sb3=new StringBuilder(usagent1);

            String usagent = sb3.toString();


            //根据创建的Schema对象，创建一行的对象
            GenericRecord record = new GenericData.Record(Text2Avro.schema);
            record.put("uip", uip);
            record.put("datetime", format);
            record.put("method", method);
            record.put("http", http);
            record.put("top", top);
            record.put("from", from);
            record.put("status1", status1);
            record.put("status2", status2);
            record.put("post", post);
            record.put("usagent", usagent);

            context.getCounter("etl_good", "good line num").increment(1L);
            System.out.println(uip+"    "+format+"    "+top+"    "+from+"    "+post+"    "+usagent+"    "+status1+"    "+status2+"    "+http);


            context.write(new AvroKey<GenericRecord>(record), NullWritable.get());


        }
    }




    @Override
    public Job getJob(Configuration conf) throws IOException {
//        // 开启reduce输出压缩
//        conf.set(FileOutputFormat.COMPRESS, "true");
//        // 设置reduce输出压缩格式
//        conf.set(FileOutputFormat.COMPRESS_CODEC, SnappyCodec.class.getName());

        Job job = Job.getInstance(conf);

        job.setJarByClass(Text2Avro.class);

        job.setMapperClass(Text2AvroMapper.class);

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(NullWritable.class);

//        无reduce
        job.setNumReduceTasks(0);

        //设置输出的format
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        //根据user_install_status.avro文件内的格式，生成指定格式的schema对象
        schema = parse.parse(Text2Avro.class.getResourceAsStream("/hainiu.avro"));

        //设置avro文件的输出
        AvroJob.setOutputKeySchema(job, schema);

        FileInputFormat.addInputPath(job, getFirstJobInputPath(conf));

        FileOutputFormat.setOutputPath(job, getOutputPath(conf));



        return job;

    }

    @Override
    public String getJobName() {


        return "etltext2avro";

    }

}
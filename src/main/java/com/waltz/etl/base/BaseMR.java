package com.waltz.etl.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import java.io.IOException;

/**
 * 公共基类
 * @author   潘牛
 * @Date	 2019年5月21日
 */
public abstract class BaseMR {

    public ControlledJob getControlledJob(Configuration conf) throws IOException{
//		1）创建ControlledJob 对象
        ControlledJob cjob = new ControlledJob(conf);

//		2）创建任务的job对象和设置job参数 -- 个性化实现
        Job job = getJob(conf);

//		4）删除当前任务的输出目录

        Path outputPath = getOutputPath(conf);

        // 自动删除输出目录
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){
            // 删除输出目录
            fs.delete(outputPath, true);
            System.out.println("delete output path:" + outputPath.toString() + " SUCCESSED!");
        }

//		5）任务的job对象与  ControlledJob 对象关联
        cjob.setJob(job);

        return cjob;
    }


    /**
     * 返回任务的job的对象，由子类去实现
     * @param conf
     * @return TODO(这里描述每个参数,如果有返回值描述返回值,如果有异常描述异常)
     */
    public abstract Job getJob(Configuration conf)throws IOException;

    /**
     * 返回基础任务名称，由子类去实现
     */
    public abstract String getJobName();

    /**
     * 个性化的任务名称
     * 基础名称+“_” + -D参数（task.id）
     * @param conf
     * @return TODO(这里描述每个参数,如果有返回值描述返回值,如果有异常描述异常)
     */
    public String getJobNameWithTaskId(Configuration conf){
        return getJobName() + "_" + conf.get(Constants.TASK_ID_ATTR);
    }


    /**
     * 获取任务链中首个任务的输入目录
     * @param conf
     * @return Path
     */
    public Path getFirstJobInputPath(Configuration conf){
        return new Path(conf.get(Constants.TASK_INPUT_DIR_ATTR));

    }


    /**
     * 获取任务链上任务的输出目录
     * @param conf
     * @return path
     */
    public Path getOutputPath(Configuration conf){
        return new Path(conf.get(Constants.TASK_BASE_PATH_ATTR) + "/" + getJobNameWithTaskId(conf));
    }
}


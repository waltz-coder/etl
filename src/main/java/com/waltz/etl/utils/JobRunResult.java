
package com.waltz.etl.utils;
import org.apache.hadoop.mapreduce.Counters;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 任务链返回信息对象
 * @author 潘牛
 * @Date 2019年5月21日
 */
public class JobRunResult {
    /**
     * true: 成功；false:失败
     */
    private boolean successed;
    /**
     * 运行时长
     * 格式化： x天x小时x分x秒
     */
    private String runTime;
    /**
     * 存失败任务名称
     */
    private List<String> failedNames = new ArrayList<String>();


    /**
     * 存每个任务的counters
     * key: jobName
     * value: 对应的counters对象
     */
    private Map<String, Counters> counterMap = new HashMap<String, Counters>();
    public boolean isSuccessed() {
        return successed;
    }
    public void setSuccessed(boolean successed) {
        this.successed = successed;
    }


    public String getRunTime() {
        return runTime;
    }


    public void setRunTime(String runTime) {
        this.runTime = runTime;
    }

    public void addFailedName(String jobName) {
        this.failedNames.add(jobName);
    }


    /**
     * 设置任务的counters
     */
    public void setCounters(String jobName, Counters counters) {
        this.counterMap.put(jobName, counters);
    }

    /**
     * 获取任务的counters
     */
    public Counters get(String jobName) {
        return this.counterMap.get(jobName);
    }


    /**
     * 打印任务链任务运行的结果信息
     * @param isPrintCounters true:打印Counters； false：不打印Counters
     */
    public void print(boolean isPrintCounters) {
        StringBuilder sb = new StringBuilder();
        sb.append("运行时长：").append(this.runTime).append("\n");

        if (this.successed) {
            sb.append("运行任务链任务：SUCCESSED").append("\n");

        } else {
            sb.append("运行任务链任务：FAILED").append("\n");
            sb.append("失败的任务列表:\n");
            for (String failedName : this.failedNames) {
                sb.append(failedName).append("\n");
            }
        }
        if (isPrintCounters) {
            sb.append("-------------------------\n");
            for (Entry<String, Counters> entry : this.counterMap.entrySet()) {
                String jobName = entry.getKey();
                Counters counters = entry.getValue();
                sb.append(jobName).append(":").append(counters).append("\n");
            }
        }
        System.out.println(sb.toString());
    }
}


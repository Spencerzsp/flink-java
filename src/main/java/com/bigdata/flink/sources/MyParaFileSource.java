package com.bigdata.flink.sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.RandomAccessFile;

/**
 * @ author spencer
 * @ date 2020/6/4 15:21
 *自定义多并行source
 */
public class MyParaFileSource extends RichParallelSourceFunction<Tuple2<String, String>> {

    private String path;

    private boolean flag = true;

    public MyParaFileSource() {
    }

    /**
     *
     * @param path /var/data/
     */
    public MyParaFileSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // /var/data/0.txt
        RandomAccessFile randomAccessFile = new RandomAccessFile(path + subtaskIndex + ".txt", "r");

        while (flag){
            String line = randomAccessFile.readLine();
            if (line != null){
                line = new String(line.getBytes("ISO-8859-1"), "UTF-8");
                ctx.collect(Tuple2.of(subtaskIndex + "", line));
            } else {
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {

        flag = false;
    }
}

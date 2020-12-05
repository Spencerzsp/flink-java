package com.bigdata.flink.bean;

/**
 * @ author spencer
 * @ date 2020/5/25 13:36
 * 相当于封装数据的bean
 */
public class WordCounts {

    public String word;
    public Long counts;

    @Override
    public String toString() {
        return "WordCounts{" +
                "word='" + word + '\'' +
                ", counts=" + counts +
                '}';
    }

    public WordCounts() {
    }

    public WordCounts(String word, Long counts) {
        this.word = word;
        this.counts = counts;
    }

    public static WordCounts of(String word, Long counts){
        return new WordCounts(word, counts);
    }

}

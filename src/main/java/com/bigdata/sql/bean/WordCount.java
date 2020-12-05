package com.bigdata.sql.bean;

/**
 * @ author spencer
 * @ date 2020/6/12 16:16
 */
public class WordCount {

    public String word;

    public Long counts;

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", counts=" + counts +
                '}';
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getCounts() {
        return counts;
    }

    public void setCounts(Long counts) {
        this.counts = counts;
    }

    public WordCount() {
    }

    public WordCount(String word, Long counts) {
        this.word = word;
        this.counts = counts;
    }
}

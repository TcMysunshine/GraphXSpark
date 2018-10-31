package com.chenhao.Graphx;

import java.io.Serializable;
import java.util.Comparator;

public class TV implements Serializable {
    private static final long serialVersionUID = -5809782578272943999L;

    private String tv;
    private Integer play;
    private Integer dm;

    public TV(String tv, Integer play, Integer dm) {
        this.tv = tv;
        this.play = play;
        this.dm = dm;
    }

    public String getTv() {
        return tv;
    }

    public void setTv(String tv) {
        this.tv = tv;
    }

    public Integer getPlay() {
        return play;
    }

    public void setPlay(Integer play) {
        this.play = play;
    }

    public Integer getDm() {
        return dm;
    }

    public void setDm(Integer dm) {
        this.dm = dm;
    }
}

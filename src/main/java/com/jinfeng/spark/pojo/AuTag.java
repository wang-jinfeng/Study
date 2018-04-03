package com.jinfeng.spark.pojo;

import java.util.List;
import java.util.Map;

/**
 * Project: Study
 * Package: com.jinfeng.spark.pojo
 * Author: wangjf
 * Date: 2018/4/1
 * Email: wangjinfeng@reyun.com
 */

public class AuTag {
    Long id;
    String id_type;
    boolean relation;
    List<Map<String, Object>> tag;
    String start;
    String end;

    public Long getId() {
        return id;
    }
    public void setId(Long id) {
        this.id = id;
    }
    public String getId_type() {
        return id_type;
    }
    public void setId_type(String id_type) {
        this.id_type = id_type;
    }
    public boolean isRelation() {
        return relation;
    }
    public void setRelation(boolean relation) {
        this.relation = relation;
    }
    public List<Map<String, Object>> getTag() {
        return tag;
    }
    public void setTag(List<Map<String, Object>> tag) {
        this.tag = tag;
    }
    public String getStart() {
        return start;
    }
    public void setStart(String start) {
        this.start = start;
    }
    public String getEnd() {
        return end;
    }
    public void setEnd(String end) {
        this.end = end;
    }
}

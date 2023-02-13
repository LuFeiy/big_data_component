package com.yuan.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ColumnMap {

    private Integer category;
    public List<String> ori_columns;
    public List<Integer> ori_indexs;
    public List<String> target_columns;

    public HashMap<String,String> rename_map;
    public HashMap<Integer,String> index_map;


    public ColumnMap(List<MapBean> list,Integer category){
        this.ori_columns = new ArrayList<>();
        this.ori_indexs = new ArrayList<>();
        this.target_columns = new ArrayList<>();
        this.rename_map = new HashMap<>();
        this.index_map = new HashMap<>();

        for (MapBean bean:list) {
            String ori_column = bean.getOri_column();
            Integer ori_index = bean.getOri_column_index();
            String tar_column = bean.getTarget_column();

            ori_columns.add(ori_column);
            ori_indexs.add(ori_index);
            target_columns.add(tar_column);

            if (category==1 && !(ori_column.equals(tar_column))){
                rename_map.put(ori_column, tar_column);
            }else if (category==2){
                index_map.put(ori_index, tar_column);
            }
        }
    }
}

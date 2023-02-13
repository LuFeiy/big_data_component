package com.yuan.joiner;

import com.yuan.bean.ColumnMap;
import com.yuan.bean.MapBean;
import joinery.DataFrame;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class JoineryTest_01 {

    //readcsv
    @Test
    public void test_01() throws IOException {
        DataFrame<Object> df = DataFrame.readCsv("src/main/resources/data/a.csv", ",");

        ListIterator<List<Object>> iterator = df.iterator();
        List<Object> next = iterator.next();
        System.out.printf(next.toString());
    }


    //retain
    @Test
    public void test_02() throws IOException {
        DataFrame<Object> df = DataFrame.readCsv("src/main/resources/data/a.csv", ",");
        DataFrame<Object> df2 = df.retain("a", "c");
        ListIterator<List<Object>> iterator = df2.iterator();
        while(iterator.hasNext()){
            List<Object> next = iterator.next();
            System.out.println(next.toString());
        }
    }

    //readcsv from io
    @Test
    public void test_03() throws IOException {
        InputStream inputStream = new FileInputStream("src/main/resources/data/a.csv");
        DataFrame<Object> df = DataFrame.readCsv(inputStream, ",","",false);
        ListIterator<List<Object>> iterator = df.iterator();
        while(iterator.hasNext()){
            List<Object> next = iterator.next();
            System.out.println(next.toString());
        }
    }

    //merge inputStream
    @Test
    public void test_04() throws IOException {
        InputStream is1 = null ;        // 输入流1
        InputStream is2 = null ;        // 输入流1
        OutputStream os = null ;        // 输出流
        SequenceInputStream sis = null ;    // 合并流
        String head = "a,b,c\n";
        is1 = new ByteArrayInputStream(head.getBytes(StandardCharsets.UTF_8));
        is2 = new FileInputStream("src/main/resources/data/b.csv") ;
        os = new FileOutputStream("src/main/resources/data/o.csv") ;
        sis = new SequenceInputStream(is1,is2) ;    // 实例化合并流
        int temp = 0 ;    // 接收内容
        while((temp=sis.read())!=-1){    // 循环输出
            os.write(temp) ;    // 保存内容
        }
        sis.close() ;    // 关闭合并流
        is1.close() ;    // 关闭输入流1`
        is2.close() ;    // 关闭输入流2
        os.close() ;    // 关闭输出流
    }

    //merge inputStream
    @Test
    public void test_05() throws IOException {
        InputStream is1 = null ;        // 输入流1
        InputStream is2 = null ;        // 输入流1
        SequenceInputStream sis = null ;    // 合并流
        String head = "a,b,c\n";
        is1 = new ByteArrayInputStream(head.getBytes(StandardCharsets.UTF_8));
        is2 = new FileInputStream("src/main/resources/data/b.csv") ;
        sis = new SequenceInputStream(is1,is2) ;    // 实例化合并流

        DataFrame<Object> df = DataFrame.readCsv(sis, ",","",false);
        ListIterator<List<Object>> iterator = df.iterator();
        while(iterator.hasNext()){
            List<Object> next = iterator.next();
            System.out.println(next.toString());
        }

        sis.close() ;    // 关闭合并流
        is1.close() ;    // 关闭输入流1`
        is2.close() ;    // 关闭输入流2
    }


    //retain by index
    @Test
    public void test_06() throws IOException {
        DataFrame<Object> df = DataFrame.readCsv("src/main/resources/data/b.csv", ",","",false);
        DataFrame<Object> df2 = df.retain(1, 2);
        ListIterator<List<Object>> iterator = df2.iterator();
        while(iterator.hasNext()){
            List<Object> next = iterator.next();
            System.out.println(next.toString());
        }
    }

    //write db
    @Test
    public void test_07() throws IOException, SQLException, ClassNotFoundException {
        DataFrame<Object> df = DataFrame.readCsv("src/main/resources/data/a.csv", ",","",true);
        ListIterator<List<Object>> iterator = df.iterator();
        while(iterator.hasNext()){
            List<Object> next = iterator.next();
            System.out.println(next.toString());
        }

        //1.加载驱动
        Class.forName("com.mysql.cj.jdbc.Driver");

        //2.建立连接
        String url = "jdbc:mysql://localhost:3306/db_test?serverTimezone=Asia/Shanghai";
        String user = "root";
        String password = "123456";
        java.sql.Connection conn = DriverManager.getConnection(url, user, password);

        String sql = "insert into df_test (a,b,c) values (?,?,?)";

        PreparedStatement stmt = conn.prepareStatement(sql);

        try {
            ParameterMetaData md = stmt.getParameterMetaData();
            List<Integer> columns = new ArrayList();

            for(int r = 0; r < df.length(); ++r) {
                for(int c = 1; c <= df.size(); ++c) {
                    stmt.setObject(c, df.get(r, c - 1));
                }

                stmt.addBatch();
            }

            stmt.executeBatch();
        } finally {
            stmt.close();
        }
        //df.writeSql(conn,sql);
        conn.close();
    }

    //dataframe 拼接
    @Test
    public void test_08() throws IOException, ClassNotFoundException, SQLException {
        String[] head = {"a", "b","c"};
        DataFrame<Object> df_head = new DataFrame<>(head);
        DataFrame<Object> df_data = DataFrame.readCsv("src/main/resources/data/b.csv", ",", "", false);
        //DataFrame<Object> df = df_head.concat(df_data);

        ListIterator<List<Object>> iterator = df_data.iterator();
        while (iterator.hasNext()){
            List<Object> next = iterator.next();
            df_head.append(next);
        }
        DataFrame<Object> df = df_head;

        //1.加载驱动
        Class.forName("com.mysql.cj.jdbc.Driver");

        //2.建立连接
        String url = "jdbc:mysql://localhost:3306/db_test?serverTimezone=Asia/Shanghai";
        String user = "root";
        String password = "123456";
        java.sql.Connection conn = DriverManager.getConnection(url, user, password);

        String sql = "insert into df_test (a,b,c) values (?,?,?)";

        PreparedStatement stmt = conn.prepareStatement(sql);

        try {
            for(int r = 0; r < df.length(); ++r) {
                for(int c = 1; c <= df.size(); ++c) {
                    stmt.setObject(c, df.get(r, c - 1));
                }
                stmt.addBatch();
            }
            stmt.executeBatch();
        } finally {
            stmt.close();
        }
        conn.close();
    }

    //算法测试-category=1
    @Test
    public void test_09() throws ClassNotFoundException, SQLException, IOException {
        //1.加载驱动
        Class.forName("com.mysql.cj.jdbc.Driver");

        //2.建立连接
        String url = "jdbc:mysql://localhost:3306/db_test?serverTimezone=Asia/Shanghai";
        String user = "root";
        String password = "123456";
        java.sql.Connection conn = DriverManager.getConnection(url, user, password);

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ori_column, ori_column_index,target_column FROM column_map");

        List<MapBean> gs = new ArrayList<MapBean>();
        MapBean g = null;
        while(rs.next()){
            g = new MapBean();
            g.setOri_column(rs.getString("ori_column"));
            g.setOri_column_index(rs.getInt("ori_column_index"));
            g.setTarget_column(rs.getString("target_column"));

            gs.add(g);
        }

        ColumnMap columnMap = new ColumnMap(gs, 1);



        DataFrame<Object> df_data = DataFrame.readCsv("src/main/resources/data/c.csv", ",", "", true);
        DataFrame<Object> retain_df = df_data.retain(columnMap.ori_columns.toArray());
        for (Map.Entry<String,String> entry:columnMap.rename_map.entrySet()){
            retain_df.rename(entry.getKey(),entry.getValue());
        }
        System.out.printf("aaa");
    }


    //算法测试-category=2
    @Test
    public void test_10() throws ClassNotFoundException, SQLException, IOException {
        //1.加载驱动
        Class.forName("com.mysql.cj.jdbc.Driver");

        //2.建立连接
        String url = "jdbc:mysql://localhost:3306/db_test?serverTimezone=Asia/Shanghai";
        String user = "root";
        String password = "123456";
        java.sql.Connection conn = DriverManager.getConnection(url, user, password);

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ori_column, ori_column_index,target_column FROM column_map where file_category=2");

        List<MapBean> gs = new ArrayList<MapBean>();
        MapBean g = null;
        while(rs.next()){
            g = new MapBean();
            g.setOri_column(rs.getString("ori_column"));
            g.setOri_column_index(rs.getInt("ori_column_index"));
            g.setTarget_column(rs.getString("target_column"));

            gs.add(g);
        }

        ColumnMap columnMap = new ColumnMap(gs, 2);



        DataFrame<Object> df_data = DataFrame.readCsv("src/main/resources/data/d.csv", ",", "", false);
        List<Integer> list = columnMap.ori_indexs;
        Integer[] aa = list.toArray(new Integer[list.size()]);
        DataFrame<Object> retain_df = df_data.retain(aa);


        List<String> target_columns = columnMap.target_columns;
        String[] head = target_columns.toArray(new String[target_columns.size()]);
        DataFrame<Object> df_head = new DataFrame<>(head);


        ListIterator<List<Object>> iterator = retain_df.iterator();
        while (iterator.hasNext()){
            List<Object> next = iterator.next();
            df_head.append(next);
        }
        DataFrame df = df_head;
        System.out.printf("aaa");
    }


    //list to array
    @Test
    public void test_11(){
        List<String>  slist = Arrays.asList("aaa","bbb","ccc");
        List<Integer> ilist = Arrays.asList(1,2,3);

        Object[] objects = slist.toArray();
        String [] sarray = slist.toArray(new String[slist.size()]);
        Integer [] iarray = ilist.toArray(new Integer[ilist.size()]);

        System.out.printf("a");


    }


}

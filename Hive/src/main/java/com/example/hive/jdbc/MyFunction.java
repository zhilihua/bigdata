package com.example.hive.jdbc;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Description:
 * @Auther: HuaZhiLi
 * @Date: 2020/12/27 20:50
 *
 * 1.先编写自定义的函数
 * 		①引入依赖
 * 		<dependency>
 * 			<groupId>org.apache.hive</groupId>
 * 			<artifactId>hive-exec</artifactId>
 * 			<version>1.2.1</version>
 * 		</dependency>
 *
 * 		②自定义UDF函数，继承UDF类
 * 		③提供evaluate()，可以提供多个重载的此方法，但是方法名是固定的
 * 		④evaluate()不能返回void，但是可以返回null!
 * 2.打包
 * 3.安装
 * 		在HIVE_HOME/auxlib 目录下存放jar包！
 * 4.创建函数
 * 		注意：用户自定义的函数，是有库的范围！指定库下创建的函数，只在当前库有效！
 *
 * 		create [temporary] function 函数名  as  自定义的函数的全类名
 */
public class MyFunction extends UDF {
    public String evaluate(String name){
        return name + " haha";
    }
}

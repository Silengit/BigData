package com.myschool.hadoop;

public class LPADriver {

    private static int times = 10;

    public static void main(String[] args) throws Exception {
        
        // LabelPropagation.main(args);
        /**
         * 有四个参数，分别是：
         * 1 入度邻接表
         * 2 输出信息（在这里不起作用）
         * 3 聚类信息第t次结果
         * 4 聚类信息第t+1次结果
         */
        String[] forItr = { args[0], args[1],"cluster.txt" };
        /**
         * 循环执行 LPA,共执行times次数, 
         * 入度邻接表作为输入文件，聚类文件作为输出文件，因为迭代的关系，将有多个输出文件，
         * 并且这些文件显示的聚类信息将越来月趋于收敛
         */
        // forItr[0] = args[0];
        // forItr[1] = args[1];
        String outPath = args[1];
        for (int i = 0; i < times; i++) {
            System.out.println("第 " + i + " 轮迭代已开始");
            
            outPath = args[1] + "/iter" + i;
            if (i == 0) {
                forItr[2] = "cluster.txt";
            }
            else {
                forItr[2] = args[1] + "/iter" + (i - 1) + "/part-r-00000";
            }
            forItr[1] = outPath;

            System.out.println("arg0 = " + forItr[0]);
            System.out.println("arg1 = " + forItr[1]);
            System.out.println("arg2 = " + forItr[2]);
            LabelPropagation.main(forItr);
        }
    }
}
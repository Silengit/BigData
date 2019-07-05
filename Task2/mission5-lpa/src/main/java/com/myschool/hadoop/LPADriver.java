package com.myschool.hadoop;

public class LPADriver {

    private static int times = 10;

    public static void main(String[] args) throws Exception {
        
        /**
         * 有四个参数，分别是：
         * 1 入度邻接表
         * 2 输出信息（在这里不起作用）
         * 3 聚类信息第t次结果
         * 4 聚类信息第t+1次结果
         */
        if (args.length != 4) {
            System.err.println("usage LPADriver: <dfs inputPath><dfs outputPath><cluster Path><cycle times>");
            return;
        }
        times = Integer.parseInt(args[3]);

        String[] forItr = { args[0], args[1],args[2] };
        /**
         * 循环执行 LPA,共执行times次数, 
         * 入度邻接表作为输入文件，聚类文件作为输出文件，因为迭代的关系，将有多个输出文件，
         * 并且这些文件显示的聚类信息将越来月趋于收敛
         * 产生的内容映射为 ：人物 -> 标签
         */
        // forItr[0] = args[0];
        // forItr[1] = args[1];
        int arch_time = 1;
        for (int i = 0; i < times; i++) {
            System.out.println("第 " + i + " 轮迭代已开始");

            forItr[1] = args[1] + "/iter" + i;
            if (i == 0) {
                forItr[2] = args[2];
            } else {
                forItr[2] = args[1] + "/iter" + (i - 1) + "/part-r-00000";
            }

            System.out.println("arg0 = " + forItr[0]);
            System.out.println("arg1 = " + forItr[1]);
            System.out.println("arg2 = " + forItr[2]);
            System.out.println();
            LabelPropagation.main(forItr);

            // 对调key-value,产生 标签->人物 的映射
            if ((i + 1) / 10 == arch_time) {
                String[] arch = { forItr[1], args[1] + "/archievement" + arch_time };
                Archievement.main(arch);
                arch_time++;
            }
        }
        if (times / 10 != arch_time) {
            String[] arch = { forItr[1], args[1] + "/archievement" + arch_time };
            Archievement.main(arch);
            arch_time++;
        }
        // String [] arch = {forItr[1],args[1] + "/archievement"};
        // Archievement.main(arch);
    }
}
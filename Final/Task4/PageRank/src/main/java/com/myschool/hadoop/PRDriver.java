package com.myschool.hadoop;

//PR驱动程序
public class PRDriver {

    private static int times = 10;

       public static void main(String[] args) throws Exception {
        times = Integer.parseInt(args[2]);
        String[] forItr = { args[0], args[1]+"/Data0" };
        PRIter.main(forItr);
        for (int i = 0; i < times; i++) {
            forItr[0] = args[1] + "/Data" + i;
            forItr[1] = args[1] + "/Data" + String.valueOf(i+1);
            PRIter.main(forItr);
        }
        String[] forRV = { args[1] + "/Data" + times, args[1] + "FinalRank" };
        PRViewer.main(forRV);
    }
}

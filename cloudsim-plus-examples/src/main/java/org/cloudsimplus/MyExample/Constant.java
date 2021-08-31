package org.cloudsimplus.MyExample;

public class Constant {
    /**
     * 数据中心参数设置
     * */
    public static final double STOP_TIME = 864000.0;    //读取多长时间的数据，默认10天，google中默认微秒为单位，cloudsimplus自动处理为秒
    public static final boolean USING_GOOGLE_HOST = true;   //使用谷歌的主机模板或者自定义主机模板
    public static final int DATACENTERS_NUMBER = 1;     //数据中心的个数

    /**
     * 数据中心代理相关参数设置
     * */
    public static final boolean SINGLE_BROKER = false;  //是否是用单一代理

    /**
     * google真实任务数据源地址
     * */
    public static final String TASK_EVENTS_PATH = "D:/paperWork/clusterdata2011/task_events";
    public static final String TASK_USAGE_PATH = "D:/paperWork/clusterdata2011/task_usage";
    public static final String LOG_FILE_PATH = "D:/java_workspace/cloudsimplus-6.3.0-MyExample/cloudsim-plus-examples/src/main/java/org/cloudsimplus/MyExample/logs/log.txt";
    public static final String MACHINE_FILENAME = "D:/paperWork/clusterdata2011/machine_events/part-00000-of-00001.csv.gz";

    /**
     * 根据google数据源host的参数比例，在参数范围内生成host
     * */
    public static final int MAX_HOST_CORES = 32;    //最大host生成的cpu数
    public static final long MAX_HOST_MEM = 32*1024; //最大host生成的内存
    public static final int MAX_HOST_BW = 100*1024;   //最大host带宽速率，用不到
    public static final int MAX_HOST_MIPS = 1000;   //最大host的mips
    public static final long MAX_HOST_STORAGE = 500*1024;    //最大host存储，用不到
    public static final boolean NUMBER_RANDOM_HOSTS = true; //只读取指定数量的随机host
    public static final int HOST_SIZE = 500;     //随机抽取指定数量的host数目
    public static final int GOOGLE_MACHINE_LINES_FILE = 12478;   //谷歌前十天machineevent所到的文件下标

    /**
     * 自己给定的host参数设置，一共有两种host
     * */
    public static final int HOSTS = 800;   //主机数量
    public static final int VMS = 2;   //虚拟机数目
    public static final int HOST_PES = 2;  //服务器核心数
    public static final long[] HOST_RAM = {4096,4096}; //内存大小
    public static final long[] HOST_BW = {1024,1024};  //带宽速率，用不到
    public static final long[] HOST_STORAGE = {1000000,1000000};  //硬盘大小，用不到
    public static final double[] HOST_MIPS = {1860,2660};  //cpu处理速率

    /**
     * cloudlet相关设置
     * 任务长度设置为负数，代表任务可以无限制运行下去，直到读到真实记录的任务结束标志
     * */
    public static final int  CLOUDLET_LENGTH = -10_000;
    public static final boolean FILTER_INSIDE_CLOUDLET = false;  //true过滤掉cloudlet里所有5~90%之外的利用率变化,false过滤掉只要出现过5~90%之外利用率的整个cloudlet
    public static final boolean READ_INITIAL_MACHINE_CLOUDLET = true;   //true则只选择在初始machine本来就上对应的cloudlet，false则分离machine和cloudlet对应关系，两者单独生成
    public static final int GOOGLE_EVENT_DAYS_FILE = 172;   //谷歌前十天taskEvent和usageEvent所到的文件下标,默认172
    public static final int GOOGLE_EVENTUSAGE_DAYS_FILE = 172;   //谷歌前十天taskEvent和usageEvent所到的文件下标,默认172
    public static final String SERIAL_CLOUDLETID_PATH = "D:/java_workspace/cloudsimplus-6.3.0-MyExample/cloudsim-plus-examples/src/main/java/org/cloudsimplus/MyExample/serialObject/cloudlets.obj";   //序列化的cloudlet ID路径（经过筛选的）
    public static final String SERIAL_PRECLOUDLETID_PATH = "D:/java_workspace/cloudsimplus-6.3.0-MyExample/cloudsim-plus-examples/src/main/java/org/cloudsimplus/MyExample/serialObject/precloudlets.obj";   //预处理的序列化的cloudlet ID路径（经过筛选的）
    public static final boolean USING_EXISTANCE_PRECLOULETS = true;    //使用外部经过预先处理的cloulet的id，不使用自己生成的
    public static final boolean USING_EXISTANCE_CLOULETS =  true;    //使用外部经过处理的cloulet的id，不使用自己生成的
    public static boolean CLOUDLETID_EXIST = false;     //默认没有序列化的cloudlet id存在

    /**
     * 标准论文vm的相关参数设置,一共有四种，根据亚马逊数据中心的虚拟机归一化得到，都是单核的
     * */
    public static final int[] VM_TYPE = {0,1,2,3};
    public static final long VM_PES = 1;
    public static final int[]  VM_MIPS = {2500,2000,1000,500};
    public static final long[] VM_RAM = {850,3750,1700,613};
    public static final long[] VM_BW = {100,100,100,100}; //用不到
    public static final long[] VM_SIZE_MB = {1000,1000,1000,1000}; //用不到
}

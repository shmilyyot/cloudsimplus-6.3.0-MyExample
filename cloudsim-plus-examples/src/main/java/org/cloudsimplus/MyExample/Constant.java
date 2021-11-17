package org.cloudsimplus.MyExample;

import org.cloudbus.cloudsim.datacenters.Datacenter;

public class Constant {
    /**
     * 数据中心参数设置
     * */
    public static final double STOP_TIME = 86400.0;    //读取多长时间的数据，默认10天，google中默认微秒为单位，cloudsimplus自动处理为秒
    public static final boolean USING_GOOGLE_HOST = false;   //使用谷歌的主机模板或者自定义主机模板
    public static final int DATACENTERS_NUMBER = 1;     //数据中心的个数
    public static final int SCHEDULING_INTERVAL = 300;   //调度时间间隔，决定系统多久更新一次cloudlet的相关状态，如vm和host的能耗，vm运行的指令数等等，不影响cloudlet的运行，越小计算越精确
    public static final int COLLECTTIME = 300;  //记录vm请求mips的间隔（利用率300秒变一次）和记录能耗的间隔

    /**
     * 数据中心日志设置
     */
    public static final boolean PRINT_LOCAL_LOG = false;    //本地打印日志
    public static final boolean PRINT_UNDERLOAD_WARN = false;   //打印低负载迁移提示
    public static int HOST_LogLength = 12;   //保留cpu和ram多长的日志信息，全部保留会内存溢出,留一个给当前时刻的利用率
    public static int VM_LogLength = HOST_LogLength;   //保留cpu和ram多长的日志信息，全部保留会内存溢出
    public static int HOST_Log_INTERVAL = SCHEDULING_INTERVAL; //    记录日志的时间间隔，默认和系统调度时间一致
    public static int VM_LOG_INTERVAL = HOST_Log_INTERVAL;  //记录虚拟机日志的时间间隔，和主机日志数目一直

    /**
     * 预测函数相关设置
     */
    public static final int KSTEP = 6;  //预测往后K个时间段的利用率

    /**
     * 数据中心迁移相关设置
     */
    public static final double HOST_CPU_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION = 0.1;    //低阈值
    public static final double HOST_CPU_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION = 0.8;     //高阈值
    /** @see Datacenter#setHostSearchRetryDelay(double) */
    public static final int HOST_SEARCH_RETRY_DELAY = 1;
    public static final double HOST_RAM_UNDER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION = 0.1;
    public static final double HOST_RAM_OVER_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION = 0.8;

    /**
     * 数据中心代理相关参数设置
     * */
    public static final boolean SINGLE_BROKER = true;  //是否是用单一代理

    /**
     * google真实任务数据源地址
     * */
    public static final String TASK_EVENTS_PATH = "D:/paperWork/clusterdata2011/task_event_process_firstDay";
    public static final String TASK_USAGE_PATH = "D:/paperWork/clusterdata2011/task_usage_process_firstDay";
    public static final String LOG_FILE_PATH = "D:/java_workspace/cloudsimplus-6.3.0-MyExample/cloudsim-plus-examples/src/main/java/org/cloudsimplus/MyExample/logs/log.txt";
    public static final String HOST_LOG_FILE_PATH = "D:/java_workspace/cloudsimplus-6.3.0-MyExample/cloudsim-plus-examples/src/main/java/org/cloudsimplus/MyExample/logs/host_utilization.txt";
    public static final String MACHINE_FILENAME = "D:/paperWork/clusterdata2011/machine_events/part-00000-of-00001.csv.gz";
//    public static final String TASK_EVENTS_PATH = "F:\\paperData\\clusterdata2011\\task_event_process_firstDay";
//    public static final String TASK_USAGE_PATH = "F:\\paperData\\clusterdata2011\\task_usage_process_firstDay";
//    public static final String LOG_FILE_PATH = "D:/java_workspace/cloudsimplus-6.3.0-MyExample/cloudsim-plus-examples/src/main/java/org/cloudsimplus/MyExample/logs/log.txt";
//    public static final String HOST_LOG_FILE_PATH = "D:/java_workspace/cloudsimplus-6.3.0-MyExample/cloudsim-plus-examples/src/main/java/org/cloudsimplus/MyExample/logs/host_utilization.txt";
//    public static final String MACHINE_FILENAME = "F:/paperData/clusterdata2011/machine_events/part-00000-of-00001.csv.gz";
    public static final boolean TEST_TRACE = false;

    /**
     * 根据google数据源host的参数比例，在参数范围内生成host
     * */
    public static final int MAX_HOST_CORES = 32;    //最大host生成的cpu数
    public static final long MAX_HOST_MEM = 32*1024; //最大host生成的内存
    public static final int MAX_HOST_BW = 100*1024;   //最大host带宽速率，用不到
    public static final int MAX_HOST_MIPS = 1000;   //最大host的mips
    public static final long MAX_HOST_STORAGE = 500*1024;    //最大host存储，用不到
    public static final boolean NUMBER_RANDOM_HOSTS = true; //只读取指定数量的随机host
    public static final int HOST_SIZE = 5;     //随机抽取指定数量的host数目
    public static final int GOOGLE_MACHINE_LINES_FILE = 12478;   //谷歌前十天machineevent所到的文件下标
    public static final double STATIC_POWER = 35;  //闲置能耗
    public static final int MAX_POWER = 50;    //最大能耗

    /**
     * 自己给定的host参数设置，一共有两种host
     * */
    public static final int HOSTS = 800;   //主机数量,请取偶数，因为默认对半分
    public static final int HOST_PES = 2;  //服务器核心数
    public static final long[] HOST_RAM = {4096,4096}; //内存大小
    public static final long[] HOST_BW = {1024 * 8,1024 * 8};  //带宽速率，用不到
    public static final long[] HOST_STORAGE = {1_000_000,1_000_000};  //硬盘大小，用不到
    public static final double[] HOST_MIPS = {1860,2660};  //cpu处理速率
    public static final Double[] HOST_G4_SPEC_POWER = {89.4,92.6,96.0,99.5,102.0,106.0,108.0,112.0,114.0,117.0};   //G4主机的spec测量功耗,开启并闲置时86
    public static final Double[] HOST_G5_SPEC_POWER = {97.0,101.0,105.0,110.0,116.0,121.0,125.0,129.0,133.0,135.0};   //G5主机的spec测量功耗,开启并闲置时93
    public static final double[] IDLE_POWER = {86,93};
    public static final double IDLE_SHUTDOWN_TIME = 0.2;    //主机闲置关机的时间


    /**
     * cloudlet相关设置
     * 任务长度设置为负数，代表任务可以无限制运行下去，直到读到真实记录的任务结束标志
     * */
    public static final int TEST_CLOUDLET_LENGTH = 200000;
    public static final boolean USING_TEST_CLOUDLET = false;
    public static final int  CLOUDLET_LENGTH = -10_000;
    public static final boolean USING_FILTER = false;
    public static final boolean FILTER_INSIDE_CLOUDLET = false;  //true过滤掉cloudlet里所有5~90%之外的利用率变化,false过滤掉只要出现过5~90%之外利用率的整个cloudlet
    public static final boolean READ_INITIAL_MACHINE_CLOUDLET = false;   //true则只选择在初始machine本来就上对应的cloudlet，false则分离machine和cloudlet对应关系，两者单独生成
    public static final int GOOGLE_EVENT_DAYS_FILE = 16;   //谷歌前十天taskEvent和usageEvent所到的文件下标,默认172，第一天16
    public static final int GOOGLE_EVENTUSAGE_DAYS_FILE = 16;   //谷歌前十天taskEvent和usageEvent所到的文件下标,默认172
//    public static final String SERIAL_CLOUDLETID_PATH = "D:/java_workspace/cloudsimplus-6.3.0-MyExample/cloudsim-plus-examples/src/main/java/org/cloudsimplus/MyExample/serialObjects/cloudlets.obj";   //序列化的cloudlet ID路径（经过筛选的）
//    public static final String SERIAL_PRECLOUDLETID_PATH = "D:/java_workspace/cloudsimplus-6.3.0-MyExample/cloudsim-plus-examples/src/main/java/org/cloudsimplus/MyExample/serialObjects/precloudlets.obj";   //预处理的序列化的cloudlet ID路径（经过筛选的）
    public static final String SERIAL_CLOUDLETID_PATH = "D:/java_workspace/cloudsimplus-6.3.0/cloudsim-plus-examples/src/main/java/org/cloudsimplus/MyExample/serialObjects/cloudlets.obj";   //序列化的cloudlet ID路径（经过筛选的）
    public static final String SERIAL_PRECLOUDLETID_PATH = "D:/java_workspace/cloudsimplus-6.3.0/cloudsim-plus-examples/src/main/java/org/cloudsimplus/MyExample/serialObjects/precloudlets.obj";   //预处理的序列化的cloudlet ID路径（经过筛选的）
    public static final boolean USING_EXISTANCE_PRECLOULETS = false;    //使用外部经过预先处理的cloulet的id，不使用自己生成的
    public static final boolean USING_EXISTANCE_CLOULETS =  false;    //使用外部经过处理的cloulet的id，不使用自己生成的
    public static boolean CLOUDLETID_EXIST = false;     //默认没有序列化的cloudlet id存在

    /**
     * 亚马逊数据中心的虚拟机归一化得到vm的相关参数设置,一共有四种，都是单核的
     * */
    public static final int VMS = 1600;   //虚拟机数目
    public static final int[] VM_TYPE = {0,1,2,3};
    public static final long VM_PES = 1;
    public static final int[]  VM_MIPS = {2500,2000,1000,500};
    public static final long[] VM_RAM = {850,1750,1750,613};    //1750效果比3750好
    public static final long[] VM_BW = {100,100,100,100}; //用不到
    public static final long[] VM_SIZE_MB = {10000,10000,10000,10000}; //用不到

    /**
     * 自定义vm的相关参数设置
     * */
    public static final int VMS_M = 1000;   //虚拟机数目
    public static final boolean USING_MODIFY_VM = true;     //true使用自定义的虚拟机规格，false使用亚马逊数据中心的虚拟机规格
    public static final long VM_PES_M = 4;
    public static final int  VM_MIPS_M = 1000;
    public static final long VM_RAM_M = 4 * 1024;
    public static final long VM_BW_M = 10*1024; //用不到
    public static final long VM_SIZE_MB_M = 50*1024; //用不到
}

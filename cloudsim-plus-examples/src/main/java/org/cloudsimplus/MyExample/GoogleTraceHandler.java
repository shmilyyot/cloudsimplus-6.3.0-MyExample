package org.cloudsimplus.MyExample;

import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudsimplus.traces.google.GoogleTaskUsageTraceReader;
import java.util.*;

public class GoogleTraceHandler {

    /**
     * google存储文件路径
     * */
    private List<String> TRACE_FILENAMES;

    public List<String> getTRACE_FILENAMES() {
        return TRACE_FILENAMES;
    }

    public void setTRACE_FILENAMES(List<String> TRACE_FILENAMES) {
        this.TRACE_FILENAMES = TRACE_FILENAMES;
    }

    public List<String> getUsage_FILENAMES() {
        return Usage_FILENAMES;
    }

    public void setUsage_FILENAMES(List<String> usage_FILENAMES) {
        Usage_FILENAMES = usage_FILENAMES;
    }

    private List<String> Usage_FILENAMES;

    public GoogleTraceHandler() {
    }

    //预处理cloudlet的id，因为一次性读入所有event再通过usage来筛选太大了，必须先再遍历一遍usage，提前把不可以的id排除出去
    public Set<Long> filterCloudletsUsageIs(CloudSim simulation){
        List<DatacenterBroker> brokers  = new ArrayList<>();
        brokers.add(new DatacenterBrokerSimple(simulation));
        Set<Long> eligibleCloudletIds = new HashSet<>(10000);
        for(String eachFileUsageName:Usage_FILENAMES){
            System.out.println("preFiltering trace usage file " + eachFileUsageName );
            GoogleTaskUsageTraceReader reader = GoogleTaskUsageTraceReader.getInstance(brokers, eachFileUsageName);
            reader.setPredicate(taskUsage ->{
                if(taskUsage.getMeanCpuUsageRate()<0.05 || taskUsage.getMeanCpuUsageRate()>0.9 ||
                    taskUsage.getCanonicalMemoryUsage()<0.05 || taskUsage.getCanonicalMemoryUsage()>0.9){
                    eligibleCloudletIds.remove(taskUsage.getUniqueTaskId());
                    return false;
                }
                eligibleCloudletIds.add(taskUsage.getUniqueTaskId());
                return false;
            });
            reader.process();
            System.out.println("Existing eligible ids : " + eligibleCloudletIds.size());
        }
        System.out.println("序列化经过usage预处理的可能cloudletid");
        return eligibleCloudletIds;
    }

    public void buildTraceFileNames(){
        TRACE_FILENAMES = new ArrayList<>(500);
        Usage_FILENAMES = new ArrayList<>(500);
        TRACE_FILENAMES.add("F:\\paperData\\clusterdata2011\\task_event_process_firstDay\\newevent.csv");
        Usage_FILENAMES.add("F:\\paperData\\clusterdata2011\\task_usage_process_firstDay\\newusage2.csv");
//        for(int i=0;i<=Constant.GOOGLE_EVENT_DAYS_FILE;++i){
//            String filename;
//            if(i<10){
//                filename = Constant.TASK_EVENTS_PATH+"/part-0000"+i+"-of-00500.csv";
//            }else if(i<100){
//                filename = Constant.TASK_EVENTS_PATH+"/part-000"+i+"-of-00500.csv";
//            }else{
//                filename = Constant.TASK_EVENTS_PATH+"/part-00"+i+"-of-00500.csv";
//            }
//            TRACE_FILENAMES.add(filename);
//        }
//        for(int i=0;i<=Constant.GOOGLE_EVENTUSAGE_DAYS_FILE;++i){
//            String usagename;
//            if(i<10){
//                usagename = Constant.TASK_USAGE_PATH+"/part-0000"+i+"-of-00500.csv.gz";
//            }else if(i<100){
//                usagename = Constant.TASK_USAGE_PATH+"/part-000"+i+"-of-00500.csv.gz";
//            }else{
//                usagename = Constant.TASK_USAGE_PATH+"/part-00"+i+"-of-00500.csv.gz";
//            }
//            Usage_FILENAMES.add(usagename);
//        }
    }

    public void buildTraceFileNamesSample(){
        TRACE_FILENAMES = new ArrayList<>(500);
        Usage_FILENAMES = new ArrayList<>(500);
        TRACE_FILENAMES.add("workload/google-traces/task-events-sample-1.csv");
        Usage_FILENAMES.add("workload/google-traces/task-usage-sample-1.csv");
    }

    public List<Host> randomChooseHostsFromGoogleHosts(List<Host> hostList,Set<Long> hostIds){
        int trueSize = Math.min(hostList.size(), Constant.HOST_SIZE);
        int totalHostNumber = hostList.size();
        Random r = new Random(System.currentTimeMillis());
        List<Host> randomHostList = new ArrayList<>(trueSize);
        Set<Integer> pickedHostIdPos = new HashSet<>(trueSize);
        while(pickedHostIdPos.size()<trueSize){
            int randomHostPos = r.nextInt(totalHostNumber);
            pickedHostIdPos.add(randomHostPos);
        }
        for(int hostIdPos:pickedHostIdPos){
            Host host = hostList.get(hostIdPos);
            randomHostList.add(host);
            hostIds.add(host.getId());
        }
        return randomHostList;
    }

}

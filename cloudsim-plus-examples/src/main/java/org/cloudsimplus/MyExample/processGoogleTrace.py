import sys
import pandas as pd
import pickle

# pd.set_option('display.max_columns', None)
TASK_EVENTS_PATH = "D:\\paperWork\\clusterdata2011\\task_events";
TASK_USAGE_PATH = "D:\\paperWork\\clusterdata2011\\task_usage";
TASK_EVENTS_PROCESS_PATH = "D:\\paperWork\\clusterdata2011\\task_events_process";
TASK_USAGE_PROCESS_PATH = "D:\\paperWork\\clusterdata2011\\task_usage_process";
# TASK_EVENTS_PATH = "F:\\paperData\\clusterdata2011\\task_events";
# TASK_USAGE_PATH = "F:\\paperData\\clusterdata2011\\task_usage";
# TASK_EVENTS_PROCESS_PATH = "F:\\paperData\\clusterdata2011\\task_events_process";
# TASK_USAGE_PROCESS_PATH = "F:\\paperData\\clusterdata2011\\task_usage_process";
event_path_list = []
usage_path_list = []
event_path_process_list = []
usage_path_process_list = []

for i in range(500):
    if i < 10:
        str_event = TASK_EVENTS_PATH+"\\part-0000"+str(i)+"-of-00500.csv.gz"
        str_usage = TASK_USAGE_PATH+"\\part-0000"+str(i)+"-of-00500.csv.gz"
        str_event_process = TASK_EVENTS_PROCESS_PATH + "\\part-0000" + str(i) + "-of-00500.csv.gz"
        str_usage_process = TASK_USAGE_PROCESS_PATH + "\\part-0000" + str(i) + "-of-00500.csv.gz"
    elif i<100:
        str_event = TASK_EVENTS_PATH + "\\part-000" + str(i) + "-of-00500.csv.gz"
        str_usage = TASK_USAGE_PATH + "\\part-000"+str(i)+"-of-00500.csv.gz"
        str_event_process = TASK_EVENTS_PROCESS_PATH + "\\part-000" + str(i) + "-of-00500.csv.gz"
        str_usage_process = TASK_USAGE_PROCESS_PATH + "\\part-000"+str(i)+"-of-00500.csv.gz"
    else:
        str_event = TASK_EVENTS_PATH + "\\part-00" + str(i) + "-of-00500.csv.gz"
        str_usage = TASK_USAGE_PATH + "\\part-00"+str(i)+"-of-00500.csv.gz"
        str_event_process = TASK_EVENTS_PROCESS_PATH + "\\part-00" + str(i) + "-of-00500.csv.gz"
        str_usage_process = TASK_USAGE_PROCESS_PATH + "\\part-00"+str(i)+"-of-00500.csv.gz"
    event_path_list.append(str_event)
    usage_path_list.append(str_usage)
    event_path_process_list.append(str_event_process)
    usage_path_process_list.append(str_usage_process)

def rateBetween(a):
    return 0.05 <= a <= 0.9
def dayBetween(timestamp):
    return timestamp < 864000000000.0

# #处理所有google事件，保证0、1、4
# for i in range(1):
#     labels = ['Timestamp','Missinginfo','jobID','Taskindex','machineID','Eventtype','username','Schedulingclass','Priority','requestCPU','requestRAM','requestdisk','constraint']
#     df = pd.read_csv(event_path_list[i],names=labels)
#     data = df[df['jobID'] == 3418309]
#     print(data)
#     data.to_csv(event_path_process_list[i],header=False, index=False)

#只要是不在5~90%利用率的usage都筛选掉
def filterOutside():
    for i in range(500):
        # df = pd.read_csv(usage_path_list[i], header=None)
        # df = df.loc(df[5].apply(rateBetween) & df[6].apply(rateBetween))
        labels = ['startTimePeriod', 'endTimePeriod', 'jobID', 'taskIndex', 'machineID', 'cpuRate',
                  'canonicalMemoryUsage', 'assignedMemoryUsage', 'unmappedMemoryUsage', 'totalMemoryUsage',
                  'maximumMemoryUsage', 'diskIOTime', 'localDiskSpaceUsed', 'maximumCPUUsage', 'maximumDiskIOTime',
                  'CPI', 'MAI', 'samplePortion', 'aggregationType', 'sampledCPUUsage']
        df = pd.read_csv(usage_path_list[i], names=labels)
        data = df.loc[(df['cpuRate'].apply(rateBetween)) & (df['canonicalMemoryUsage'].apply(rateBetween))]
        data.to_csv(usage_path_process_list[i], header=False, index=False)
        print('完成过滤第'+str(i)+'个usage文档')

def collectFilterOutsideCloudletIds():
    cloudIdsSet = set()
    for i in range(500):
        labels = ['startTimePeriod', 'endTimePeriod', 'jobID', 'taskIndex', 'machineID', 'cpuRate',
                  'canonicalMemoryUsage', 'assignedMemoryUsage', 'unmappedMemoryUsage', 'totalMemoryUsage',
                  'maximumMemoryUsage', 'diskIOTime', 'localDiskSpaceUsed', 'maximumCPUUsage', 'maximumDiskIOTime',
                  'CPI', 'MAI', 'samplePortion', 'aggregationType', 'sampledCPUUsage']
        df = pd.read_csv(usage_path_process_list[i], names=labels)
        for row in df.itertuples():
            cloudIdsSet.add(getattr(row, 'taskIndex'))
        print('完成生成第' + str(i) + '个usage文档的cloudlet ID')
    print(len(cloudIdsSet))
    with open('D:/paperWork/clusterdata2011/task_usage_process/cloudletIdsSetFitOutside.pickle', 'wb') as f:
        pickle.dump(cloudIdsSet, f)

def collectFilterOutsideFirstTenDayCloudletIds():
    cloudIdsSet = set()
    for i in range(173):
        labels = ['startTimePeriod', 'endTimePeriod', 'jobID', 'taskIndex', 'machineID', 'cpuRate',
                  'canonicalMemoryUsage', 'assignedMemoryUsage', 'unmappedMemoryUsage', 'totalMemoryUsage',
                  'maximumMemoryUsage', 'diskIOTime', 'localDiskSpaceUsed', 'maximumCPUUsage', 'maximumDiskIOTime',
                  'CPI', 'MAI', 'samplePortion', 'aggregationType', 'sampledCPUUsage']
        df = pd.read_csv(usage_path_process_list[i], names=labels)
        for row in df.itertuples():
            if dayBetween(getattr(row, 'endTimePeriod')):
                cloudIdsSet.add(getattr(row, 'taskIndex'))
        print('完成生成第' + str(i) + '个usage文档的cloudlet ID')
    print(len(cloudIdsSet))
    with open('D:/paperWork/clusterdata2011/task_usage_process/cloudletIdsSetFitOutsideFirstTenDay.pickle', 'wb') as f:
        pickle.dump(cloudIdsSet, f)

#只要有一个利用率不在5~90%里面，直接这个cloudid都移除出去
def filterInside():
    cloudIdsSet = set()
    for i in range(500):
        labels = ['startTimePeriod', 'endTimePeriod', 'jobID', 'taskIndex', 'machineID', 'cpuRate',
                  'canonicalMemoryUsage', 'assignedMemoryUsage', 'unmappedMemoryUsage', 'totalMemoryUsage',
                  'maximumMemoryUsage', 'diskIOTime', 'localDiskSpaceUsed', 'maximumCPUUsage', 'maximumDiskIOTime',
                  'CPI', 'MAI', 'samplePortion', 'aggregationType', 'sampledCPUUsage']
        df = pd.read_csv(usage_path_list[i], names=labels)
        for row in df.itertuples():
            if rateBetween(getattr(row,'cpuRate')) and rateBetween(getattr(row,'canonicalMemoryUsage')):
                cloudIdsSet.add(getattr(row,'taskIndex'))
            else:
                cloudIdsSet.discard(getattr(row,'taskIndex'))
        print('完成过滤第'+str(i)+'个usage文档')
    print(len(cloudIdsSet))
    with open('F:/paperData/clusterdata2011/task_usage_process2/cloudletIdsSet.pickle','wb') as f:
        pickle.dump(cloudIdsSet,f)

def AfterFilterInside():
    with open('F:/paperData/clusterdata2011/task_usage_process2/cloudletIdsSet.pickle','rb') as f:
        cloudletIdsSet = pickle.load(f)
    for i in range(500):
        labels = ['startTimePeriod', 'endTimePeriod', 'jobID', 'taskIndex', 'machineID', 'cpuRate',
                  'canonicalMemoryUsage', 'assignedMemoryUsage', 'unmappedMemoryUsage', 'totalMemoryUsage',
                  'maximumMemoryUsage', 'diskIOTime', 'localDiskSpaceUsed', 'maximumCPUUsage', 'maximumDiskIOTime',
                  'CPI', 'MAI', 'samplePortion', 'aggregationType', 'sampledCPUUsage']
        df = pd.read_csv(usage_path_list[i], names=labels)
        data = df[df['taskIndex'].isin(cloudletIdsSet)]
        data.to_csv(usage_path_process_list[i], header=False, index=False)
        print('完成过滤第'+str(i)+'个usage文档')


if __name__ == '__main__':
    # filterOutside()
    # collectFilterOutsideCloudletIds()
    collectFilterOutsideFirstTenDayCloudletIds()
    # filterInside()
    # AfterFilterInside()
import sys
import pandas as pd
import pickle
from random import choice
import random
import csv

# pd.set_option('display.max_columns', None)
# TASK_EVENTS_PATH = "D:\\paperWork\\clusterdata2011\\task_events";
# TASK_USAGE_PATH = "D:\\paperWork\\clusterdata2011\\task_usage";
# TASK_EVENTS_PROCESS_PATH = "D:\\paperWork\\clusterdata2011\\finishId";
# TASK_USAGE_PROCESS_PATH = "D:\\paperWork\\clusterdata2011\\task_usage_process_firstDay_Between_Ramdom1000";
TASK_EVENTS_PATH = "F:\\paperData\\clusterdata2011\\task_events";
TASK_USAGE_PATH = "F:\\paperData\\clusterdata2011\\task_usage";
TASK_EVENTS_PROCESS_PATH = "F:\\paperData\\clusterdata2011\\task_event_process_firstDay";
TASK_USAGE_PROCESS_PATH = "F:\\paperData\\clusterdata2011\\task_usage_process_firstDay";
event_path_list = []
usage_path_list = []
event_path_process_list = []
usage_path_process_list = []

for i in range(500):
    if i < 10:
        str_event = TASK_EVENTS_PATH + "\\part-0000" + str(i) + "-of-00500.csv.gz"
        str_usage = TASK_USAGE_PATH + "\\part-0000" + str(i) + "-of-00500.csv.gz"
        str_event_process = TASK_EVENTS_PROCESS_PATH + "\\part-0000" + str(i) + "-of-00500.csv"
        str_usage_process = TASK_USAGE_PROCESS_PATH + "\\part-0000" + str(i) + "-of-00500.csv.gz"
    elif i < 100:
        str_event = TASK_EVENTS_PATH + "\\part-000" + str(i) + "-of-00500.csv.gz"
        str_usage = TASK_USAGE_PATH + "\\part-000" + str(i) + "-of-00500.csv.gz"
        str_event_process = TASK_EVENTS_PROCESS_PATH + "\\part-000" + str(i) + "-of-00500.csv"
        str_usage_process = TASK_USAGE_PROCESS_PATH + "\\part-000" + str(i) + "-of-00500.csv.gz"
    else:
        str_event = TASK_EVENTS_PATH + "\\part-00" + str(i) + "-of-00500.csv.gz"
        str_usage = TASK_USAGE_PATH + "\\part-00" + str(i) + "-of-00500.csv.gz"
        str_event_process = TASK_EVENTS_PROCESS_PATH + "\\part-00" + str(i) + "-of-00500.csv"
        str_usage_process = TASK_USAGE_PROCESS_PATH + "\\part-00" + str(i) + "-of-00500.csv.gz"
    event_path_list.append(str_event)
    usage_path_list.append(str_usage)
    event_path_process_list.append(str_event_process)
    usage_path_process_list.append(str_usage_process)


def rateBetween(a):
    return 0.05 <= a <= 0.9

def rateOutside(a):
    return a >= 0.9 or a <= 0.05


def dayBetween(timestamp):
    return timestamp <= 86400000000.0


def rateBigger(a):
    return 0.5 <= a


# #处理所有google事件，保证0、1、4
# for i in range(1):
#     labels = ['Timestamp','Missinginfo','jobID','Taskindex','machineID','Eventtype','username','Schedulingclass','Priority','requestCPU','requestRAM','requestdisk','constraint']
#     df = pd.read_csv(event_path_list[i],names=labels)
#     data = df[df['jobID'] == 3418309]
#     print(data)
#     data.to_csv(event_path_process_list[i],header=False, index=False)

# 只要是不在5~90%利用率的usage都筛选掉
def filterOutside():
    for i in range(500):
        # df = pd.read_csv(usage_path_list[i], header=None)
        # df = df.loc(df[5].apply(rateBetween) & df[6].apply(rateBetween))
        labels = ['startTimePeriod', 'endTimePeriod', 'jobID', 'taskIndex', 'machineID', 'cpuRate',
                  'canonicalMemoryUsage', 'assignedMemoryUsage', 'unmappedMemoryUsage', 'totalMemoryUsage',
                  'maximumMemoryUsage', 'diskIOTime', 'localDiskSpaceUsed', 'maximumCPUUsage', 'maximumDiskIOTime',
                  'CPI', 'MAI', 'samplePortion', 'aggregationType', 'sampledCPUUsage']
        df = pd.read_csv(usage_path_list[i], names=labels)
        data = df.loc[(df['cpuRate'].apply(rateBigger)) | (df['canonicalMemoryUsage'].apply(rateBigger))]
        data.to_csv(usage_path_process_list[i], header=False, index=False)
        print('完成过滤第' + str(i) + '个usage文档')


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


def collectCloudletIdInFirstDay():
    cloudIdsSet = set()
    flag = True
    for i in range(17):
        labels = ['Timestamp','Missinginfo','jobID','Taskindex','machineID','Eventtype','username','Schedulingclass','Priority','requestCPU','requestRAM','requestdisk','constraint']
        df = pd.read_csv(event_path_list[i], names=labels)
        for row in df.itertuples():
            if dayBetween(getattr(row, 'Timestamp')):
                cloudIdsSet.add(getattr(row, 'jobID')+getattr(row, 'Taskindex'))
            else:
                break
        print('完成过滤第' + str(i) + '个event文档')
        print('当前event数目：' + str(len(cloudIdsSet)))
    with open('F:/paperData/clusterdata2011/task_events/totalCloudletIdsSetInFirstDay.pickle', 'wb') as f:
        pickle.dump(cloudIdsSet, f)



def collectCloudletIdInExtermly():
    cloudIdsSet = set()
    for i in range(17):
        labels = ['startTimePeriod', 'endTimePeriod', 'jobID', 'taskIndex', 'machineID', 'cpuRate',
                  'canonicalMemoryUsage', 'assignedMemoryUsage', 'unmappedMemoryUsage', 'totalMemoryUsage',
                  'maximumMemoryUsage', 'diskIOTime', 'localDiskSpaceUsed', 'maximumCPUUsage', 'maximumDiskIOTime',
                  'CPI', 'MAI', 'samplePortion', 'aggregationType', 'sampledCPUUsage']
        df = pd.read_csv(usage_path_list[i], names=labels)
        for row in df.itertuples():
            if rateBigger(float(getattr(row, 'cpuRate'))) or rateBigger(float(getattr(row, 'canonicalMemoryUsage'))):
                cloudIdsSet.add(getattr(row, 'jobID')+getattr(row, 'taskIndex'))
        print('完成过滤第' + str(i) + '个usage文档')
        print('当前大于0.5的id数目：' + str(len(cloudIdsSet)))
    with open('D:/paperWork/clusterdata2011/task_usage_process2/totalCloudletIdsSetInFirstDayExterm.pickle', 'wb') as f:
        pickle.dump(cloudIdsSet, f)


# 只要有一个利用率不在5~90%里面，直接这个cloudid都移除出去,前十天
def filterInside():
    with open('D:/paperWork/clusterdata2011/task_usage_process2/totalCloudletIdsSetInFirstDay.pickle', 'rb') as f:
        cloudIdsSet = pickle.load(f)
    print('当前cloudlet数目：' + str(len(cloudIdsSet)))
    flag = True
    for i in range(500):
        labels = ['startTimePeriod', 'endTimePeriod', 'jobID', 'taskIndex', 'machineID', 'cpuRate',
                  'canonicalMemoryUsage', 'assignedMemoryUsage', 'unmappedMemoryUsage', 'totalMemoryUsage',
                  'maximumMemoryUsage', 'diskIOTime', 'localDiskSpaceUsed', 'maximumCPUUsage', 'maximumDiskIOTime',
                  'CPI', 'MAI', 'samplePortion', 'aggregationType', 'sampledCPUUsage']
        df = pd.read_csv(usage_path_list[i], names=labels)
        for row in df.itertuples():
            if dayBetween(getattr(row, 'startTimePeriod')):
                if rateOutside(getattr(row, 'cpuRate')) and rateOutside(getattr(row, 'canonicalMemoryUsage')):
                    cloudIdsSet.discard(getattr(row, 'jobID') + getattr(row, 'taskIndex'))
            else:
                flag = False
                break
        if not flag:
            print('完成过滤第' + str(i) + '个usage文档')
            print('当前cloudlet数目：' + str(len(cloudIdsSet)))
            break
        print('完成过滤第' + str(i) + '个usage文档')
        print('当前cloudlet数目：' + str(len(cloudIdsSet)))
    print(len(cloudIdsSet))
    with open('D:/paperWork/clusterdata2011/task_usage_process2/cloudletIdsSetFisrtDayBetweenStrict.pickle', 'wb') as f:
        pickle.dump(cloudIdsSet, f)


def AfterFilterInside():
    # with open('D:/paperWork/clusterdata2011/task_usage_process2/totalCloudletIdsSetInFirstDay.pickle', 'rb') as f:
    #     totalCloudIdsSet = pickle.load(f)
    # with open('D:/paperWork/clusterdata2011/task_usage_process2/cloudletIdsSetFisrtDayBetween.pickle', 'rb') as f:
    #     cloudletIdsSet1 = pickle.load(f)
    # with open('D:/paperWork/clusterdata2011/task_usage_process2/totalCloudletIdsSetInFirstDayExtermLarge.pickle', 'rb') as f:
    #     cloudletIdsSet2 = pickle.load(f)
    #
    #
    #
    # cloudletIdsSet1 = set(random.sample(list(cloudletIdsSet1), 1000))
    # cloudletIdsSet2 = set(random.sample(list(cloudletIdsSet2), 100))
    # cloudletIdsSet3 = set(random.sample(list(cloudletIdsSet1), 1000))
    # finalset = cloudletIdsSet1.union(cloudletIdsSet2)
    # while not checkExist(finalset, totalCloudIdsSet):
    #     cloudletIdsSet1 = set(random.sample(list(cloudletIdsSet1), 1000))
    #     cloudletIdsSet2 = set(random.sample(list(cloudletIdsSet2), 100))
    #     finalset = cloudletIdsSet1.union(cloudletIdsSet2)
    # with open('D:/paperWork/clusterdata2011/task_usage_process2/fianlRandomMergeIds.pickle', 'wb') as f:
    #     pickle.dump(finalset, f)
    # with open('D:/paperWork/clusterdata2011/task_usage_process2/fianlRandomIds.pickle', 'wb') as f:
    #     pickle.dump(cloudletIdsSet3, f)
    with open('D:/paperWork/clusterdata2011/task_usage_process2/ActualFianlRandomMergeIds.pickle', 'rb') as f:
        cloudIdsSet = pickle.load(f)
    for i in range(17):
        labels = ['startTimePeriod', 'endTimePeriod', 'jobID', 'taskIndex', 'machineID', 'cpuRate',
                  'canonicalMemoryUsage', 'assignedMemoryUsage', 'unmappedMemoryUsage', 'totalMemoryUsage',
                  'maximumMemoryUsage', 'diskIOTime', 'localDiskSpaceUsed', 'maximumCPUUsage', 'maximumDiskIOTime',
                  'CPI', 'MAI', 'samplePortion', 'aggregationType', 'sampledCPUUsage']
        df = pd.read_csv(usage_path_list[i], names=labels)
        # for row in df.itertuples():
        #     uniqueId = getattr(row, 'jobID') + getattr(row, 'taskIndex')
        #     if uniqueId in cloudletIdsSet:

        data = df[(df['jobID']+df['taskIndex']).isin(cloudIdsSet)]
        data.to_csv(usage_path_process_list[i], header=False, index=False)
        print('完成过滤第' + str(i) + '个usage文档')

def filterEventInMergeRandom1000Ids():
    with open('D:/paperWork/clusterdata2011/task_usage_process2/ActualFianlRandomMergeIds.pickle', 'rb') as f:
        cloudIdsSet = pickle.load(f)
    for i in range(17):
        labels = ['Timestamp','Missinginfo','jobID','Taskindex','machineID','Eventtype','username','Schedulingclass','Priority','requestCPU','requestRAM','requestdisk','constraint']
        df = pd.read_csv(event_path_list[i], names=labels)
        data = df[(df['jobID']+df['Taskindex']).isin(cloudIdsSet)]
        data.to_csv(event_path_process_list[i], header=False, index=False)
        print('完成过滤第' + str(i) + '个event文档')

def collectfinishId():
    with open('D:/paperWork/clusterdata2011/task_usage_process2/ActualFianlRandomMergeIds.pickle', 'rb') as f:
        cloudIdsSet = pickle.load(f)
    print((len(cloudIdsSet)))
    dict1 = {}
    dict2 = {}
    # tempset = set()
    for i in range(17):
        labels = ['Timestamp','Missinginfo','jobID','Taskindex','machineID','Eventtype','username','Schedulingclass','Priority','requestCPU','requestRAM','requestdisk','constraint']
        df = pd.read_csv(event_path_process_list[i], names=labels)
        for row in df.itertuples():
            uniqueId = getattr(row, 'jobID')+getattr(row, 'Taskindex')
            if uniqueId in cloudIdsSet:
                dict1[uniqueId] = getattr(row, 'jobID')
                dict2[uniqueId] = getattr(row, 'Taskindex')
                if getattr(row, 'Eventtype') == 4:
                    cloudIdsSet.discard(uniqueId)
        print('完成过滤第' + str(i) + '个event文档')
    print("当前还有：" + str(len(cloudIdsSet)) + "个id没有finish")
    # cloudletIdsSet1 = set(random.sample(list(tempset), 1000))
    # with open('D:/paperWork/clusterdata2011/task_usage_process2/ActualFianlRandomMergeIds.pickle', 'wb') as f:
    #     pickle.dump(cloudletIdsSet1,f)
    # print(len(cloudletIdsSet1))
    print(len(dict1))
    with open("D:/paperWork/clusterdata2011/finishId/part-00016-of-00500.csv", "a",newline='') as csvfile:
        writer = csv.writer(csvfile)
        for num in cloudIdsSet:
            row = [85593000000, '', dict1[num], dict2[num], 0, 4, "Xalks/empsl1000/0tursl=", 0, 0, 0.0625, 0.0318,
                   0.0007715, 0]
            writer.writerow(row)

def checkExist(a,b):
    for num in a:
        if num not in b:
            return False
    return True


def randomSelectCloudlets():
    cloudletNum = 1600
    with open('F:/paperData/clusterdata2011/task_events/totalCloudletIdsSetInFirstDay.pickle', 'rb') as f:
        cloudIdsSet = pickle.load(f)
    cloudletRandomSet = set(random.sample(list(cloudIdsSet), cloudletNum))
    with open('F:/paperData/clusterdata2011/task_events/ramdomChooseCloudletsInFirstDay.pickle', 'wb') as f:
        pickle.dump(cloudletRandomSet, f)

def filterRamdomCloudletEvents():
    with open('F:/paperData/clusterdata2011/task_events/ramdomChooseCloudletsInFirstDay.pickle', 'rb') as f:
        cloudIdsSet = pickle.load(f)
    for i in range(17):
        labels = ['Timestamp', 'Missinginfo', 'jobID', 'Taskindex', 'machineID', 'Eventtype', 'username',
                  'Schedulingclass', 'Priority', 'requestCPU', 'requestRAM', 'requestdisk', 'constraint']
        df = pd.read_csv(event_path_list[i], names=labels)
        data = df[(df['jobID'] + df['Taskindex']).isin(cloudIdsSet)]
        if(len(data) != 0):
            data.to_csv(event_path_process_list[i], header=False, index=False)
        print('完成过滤第' + str(i) + '个event文档')

def filterRamdomCloudletUsages():
    with open('F:/paperData/clusterdata2011/task_events/ramdomChooseCloudletsInFirstDay.pickle', 'rb') as f:
        cloudIdsSet = pickle.load(f)
    for i in range(17):
        labels = ['startTimePeriod', 'endTimePeriod', 'jobID', 'taskIndex', 'machineID', 'cpuRate',
                  'canonicalMemoryUsage', 'assignedMemoryUsage', 'unmappedMemoryUsage', 'totalMemoryUsage',
                  'maximumMemoryUsage', 'diskIOTime', 'localDiskSpaceUsed', 'maximumCPUUsage', 'maximumDiskIOTime',
                  'CPI', 'MAI', 'samplePortion', 'aggregationType', 'sampledCPUUsage']
        df = pd.read_csv(usage_path_list[i], names=labels)
        data = df[(df['jobID'] + df['taskIndex']).isin(cloudIdsSet)]
        data.to_csv(usage_path_process_list[i], header=False, index=False)
        print('完成过滤第' + str(i) + '个usage文档')

def mergeUsageIntoOneFile():
    for i in range(17):
        labels = ['startTimePeriod', 'endTimePeriod', 'jobID', 'taskIndex', 'machineID', 'cpuRate',
                  'canonicalMemoryUsage', 'assignedMemoryUsage', 'unmappedMemoryUsage', 'totalMemoryUsage',
                  'maximumMemoryUsage', 'diskIOTime', 'localDiskSpaceUsed', 'maximumCPUUsage', 'maximumDiskIOTime',
                  'CPI', 'MAI', 'samplePortion', 'aggregationType', 'sampledCPUUsage']
        df = pd.read_csv(usage_path_process_list[i], names=labels)
        df.to_csv('F:/paperData/clusterdata2011/task_usage_process_firstDay/usage.csv',mode = 'a',header=False, index=False)
        print('finish merge'+str(i))

def mergeEventIntoOneFile():
    for i in range(17):
        labels = ['Timestamp', 'Missinginfo', 'jobID', 'Taskindex', 'machineID', 'Eventtype', 'username',
                  'Schedulingclass', 'Priority', 'requestCPU', 'requestRAM', 'requestdisk', 'constraint']
        df = pd.read_csv(event_path_process_list[i], names=labels)
        df.to_csv('F:/paperData/clusterdata2011/task_event_process_firstDay/event.csv', mode='a', header=False, index=False)
        print('finish merge' + str(i))


if __name__ == '__main__':
    # collectCloudletIdInFirstDay()
    # filterOutside()
    # collectFilterOutsideCloudletIds()
    # collectFilterOutsideFirstTenDayCloudletIds()
    # checkIfOverload()
    # filterInside()
    # AfterFilterInside()
    # collectCloudletIdInExtermly()
    # filterEventInMergeRandom1000Ids()
    # collectfinishId()
    # randomSelectCloudlets()
    # filterRamdomCloudletEvents()
    # filterRamdomCloudletUsages()
    mergeEventIntoOneFile()
    mergeUsageIntoOneFile()
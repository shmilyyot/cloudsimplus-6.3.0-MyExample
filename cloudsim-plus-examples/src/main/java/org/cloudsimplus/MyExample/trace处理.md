### 首先过滤usage，找到所有cpu或ram利用率大于0.5的usage，然后从中随机抽取1000个task id，然后再按照这些task id记录下来所有usage

### 根据task id去过滤event，保留event调度信息（只用那些一开始就生成的）


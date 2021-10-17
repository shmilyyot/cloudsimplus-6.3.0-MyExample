package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.HashMap;
import java.util.List;

public class VmSelectionPolicyUnbalanceUtilization implements VmSelectionPolicy {
    @Override
    public Vm getVmToMigrate(Host host) {
        final List<Vm> migratableVms = host.getMigratableVms();
        if(migratableVms.isEmpty())
            return Vm.NULL;
        Vm vmToMigrate = Vm.NULL;
        double maxWastage = Double.MIN_VALUE;
        for(Vm vm:migratableVms){
            if(vm.isInMigration() || vm.getCloudletScheduler().isEmpty()){
                continue;
            }
            double wastage = resourceWastage(host,vm);
            if(wastage > maxWastage){
                maxWastage = wastage;
                vmToMigrate = vm;
            }
        }
        return vmToMigrate;
    }

    public double resourceWastage(Host host,Vm vm){
        double xita = 0.0001;
        double hostCpuCapacity = host.getTotalMipsCapacity();
        double hostRamCapacity = host.getRam().getCapacity();
        double hostCpuUtilization = host.getCpuPercentUtilization();
        double hostRamUtilization = host.getRamPercentUtilization();
        double vmCpuUsage = vm.getCpuPercentUtilization() * vm.getCurrentRequestedTotalMips();
        double vmRamUsage = vm.getCurrentRequestedRam();
        double hostRemindingCpuUtilization = (hostCpuUtilization * hostCpuCapacity - vmCpuUsage) / hostCpuCapacity;
        double hostRemindingRamUtilization = (hostRamUtilization * hostRamCapacity - vmRamUsage) / hostRamCapacity;
        double wastage = (Math.abs(hostRemindingCpuUtilization - hostRemindingRamUtilization) + xita) / (hostCpuUtilization + hostRamUtilization);
//        System.out.println("remove "+vm +" in "+host+" wastage :" + wastage);
        return wastage;
    }
}

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
        double minWastage = Double.MAX_VALUE;
        for(Vm vm:migratableVms){
            if(vm.isInMigration() || vm.getCloudletScheduler().isEmpty()){
                continue;
            }
            double wastage = resourceWastage(host,vm);
            if(wastage < minWastage){
                minWastage = wastage;
                vmToMigrate = vm;
            }
        }
        return vmToMigrate;
    }

    public double resourceWastage(Host host,Vm vm){
        double xita = 0.0001;
        double hostCpuCapacity = host.getTotalMipsCapacity();
        double hostRamCapacity = host.getRam().getCapacity();
        double hostCpuUtilization = Math.min(host.getCpuPercentUtilization(),1.0);
        double hostRamUtilization = Math.min(host.getRamPercentUtilization(),1.0);
        if(hostCpuUtilization == 0.0 || hostRamUtilization == 0.0) return 1.0;
        double vmCpuUsage = vm.getCurrentUtilizationTotalMips();
        double vmRamUsage = vm.getCurrentRequestedRam();
        double newhostCpuUtilization = (hostCpuUtilization * hostCpuCapacity - vmCpuUsage) / hostCpuCapacity;
        double newhostRamUtilization = (hostRamUtilization * hostRamCapacity - vmRamUsage) / hostRamCapacity;
        double hostRemindingCpuUtilization = (hostCpuCapacity - (hostCpuUtilization * hostCpuCapacity - vmCpuUsage)) / hostCpuCapacity;
        double hostRemindingRamUtilization = (hostRamCapacity - (hostRamUtilization * hostRamCapacity - vmRamUsage)) / hostRamCapacity;
        double wastage = (Math.abs(hostRemindingCpuUtilization - hostRemindingRamUtilization) + xita) / (newhostCpuUtilization + newhostRamUtilization);
//        System.out.println("remove "+vm +" in "+host+" wastage :" + wastage);
        return wastage * vm.getCurrentRequestedRam();
//        return wastage;
    }
}

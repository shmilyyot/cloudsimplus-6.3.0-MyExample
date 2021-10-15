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
        return 0.0;
    }
}

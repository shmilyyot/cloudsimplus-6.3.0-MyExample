package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.List;

public class VmSelectionPolicyMM implements VmSelectionPolicy {
    @Override
    public Vm getVmToMigrate(final Host host) {
        final List<? extends Vm> migratableVms = host.getMigratableVms();
        if (migratableVms.isEmpty()) {
            return Vm.NULL;
        }
        migratableVms.sort((a,b)-> (int) (b.getCpuPercentUtilization() - a.getCpuPercentUtilization()));
        Vm bestvm = Vm.NULL;
        double hutil = host.getCpuPercentUtilization();
        double bestUtil = Double.MAX_VALUE;
        double t = 0.0;
        for(Vm vm:migratableVms) {
            if(vm.getCpuPercentUtilization() * vm.getMips() / host.getTotalMipsCapacity() > hutil - host.getCPU_THRESHOLD()) {
                t = vm.getCpuPercentUtilization() * vm.getMips() / host.getTotalMipsCapacity() - hutil + host.getCPU_THRESHOLD();
                if(t < bestUtil){
                    bestUtil = t;
                    bestvm = vm;
                }
            }
            else{
                if(bestUtil == Double.MAX_VALUE){
                    bestvm = vm;
                }
                break;
            }
        }
//        final Predicate<Vm> inMigration = Vm::isInMigration;
//        final Optional<? extends Vm> optional = migratableVms.stream()
//            .filter(inMigration.negate())
//            .findFirst();
        return bestvm;
    }
}

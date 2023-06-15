package org.cloudsimplus.MyExample.modifyMigration;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class VmSelectionPolicyCFS implements VmSelectionPolicy {
    @Override
    public Vm getVmToMigrate(final Host host) {
        final List<? extends Vm> migratableVms = host.getMigratableVms();
        if (migratableVms.isEmpty()) {
            return Vm.NULL;
        }

//        final Predicate<Vm> inMigration = Vm::isInMigration;
//        final Optional<? extends Vm> optional = migratableVms.stream()
//            .filter(inMigration.negate())
//            .findFirst();
        return migratableVms.get(0);
    }
}

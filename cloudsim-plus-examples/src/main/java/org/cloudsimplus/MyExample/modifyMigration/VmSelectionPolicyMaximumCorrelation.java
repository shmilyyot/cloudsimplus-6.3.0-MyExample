package org.cloudsimplus.MyExample.modifyMigration;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicyMinimumMigrationTime;
import org.cloudbus.cloudsim.util.MathUtil;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class VmSelectionPolicyMaximumCorrelation implements VmSelectionPolicy {

    private VmSelectionPolicy fallbackPolicy;
    private final Map<Vm,LinkedList<Double>> allVmsRamUtilizationHistoryQueue;
    private final Map<Vm,LinkedList<Double>> allVmsCpuUtilizationHistoryQueue;

    public VmSelectionPolicyMaximumCorrelation(final VmSelectionPolicy fallbackPolicy,final Map<Vm,LinkedList<Double>> allVmsRamUtilizationHistoryQueue,final Map<Vm,LinkedList<Double>> allVmsCpuUtilizationHistoryQueue) {
        super();
        setFallbackPolicy(fallbackPolicy);
        this.allVmsRamUtilizationHistoryQueue = allVmsRamUtilizationHistoryQueue;
        this.allVmsCpuUtilizationHistoryQueue = allVmsCpuUtilizationHistoryQueue;
    }

    @Override
    public Vm getVmToMigrate(Host host) {
        final List<Vm> migratableVms = host.getMigratableVms();
        if(migratableVms.isEmpty())
            return Vm.NULL;
        Vm vmToMigrate = Vm.NULL;
        List<Double> metrics;
        try {
            metrics = getCorrelationCoefficients(getUtilizationMatrix(migratableVms));
        } catch (IllegalArgumentException e) {
            if(fallbackPolicy == null){
                fallbackPolicy = new VmSelectionPolicyMinimumMigrationTime();
            }
            return getFallbackPolicy().getVmToMigrate(host);
        }
        double maxMetric = Double.MIN_VALUE;
        for (int i = 0; i < metrics.size(); i++) {
            Vm tempVm = migratableVms.get(i);
            if(tempVm.isInMigration() || tempVm.getCloudletScheduler().isEmpty()){
                continue;
            }
            double metric = metrics.get(i);
            if (metric > maxMetric) {
                maxMetric = metric;
                vmToMigrate = tempVm;
            }
        }
        return vmToMigrate;
    }

    protected double[][] getUtilizationMatrix(final List<Vm> vmList) {
        int n = vmList.size();
        int m = getMinUtilizationHistorySize(vmList);
        double[][] utilization = new double[n][m];
        for (int i = 0; i < n; i++) {
            List<Double> vmUtilization = allVmsCpuUtilizationHistoryQueue.get(vmList.get(i));
            for (int j = 0; j < vmUtilization.size(); j++) {
                utilization[i][j] = vmUtilization.get(j);
            }
        }
        return utilization;
    }

    /**
     * Gets the min CPU utilization percentage history size among a list of VMs.
     *
     * @param vmList the VM list
     * @return the min CPU utilization percentage history size of the VM list
     */
    protected int getMinUtilizationHistorySize(final List<Vm> vmList) {
        int minSize = Integer.MAX_VALUE;
        for (Vm vm : vmList) {
            int size = allVmsCpuUtilizationHistoryQueue.get(vm).size();
            if (size < minSize) {
                minSize = size;
            }
        }
        return minSize;
    }

    /**
     * Gets the correlation coefficients.
     *
     * @param data the data
     * @return the correlation coefficients
     */
    protected List<Double> getCorrelationCoefficients(final double[][] data) {
        int n = data.length;
        int m = data[0].length;
        List<Double> correlationCoefficients = new LinkedList<>();
        for (int i = 0; i < n; i++) {
            double[][] x = new double[n - 1][m];
            int k = 0;
            for (int j = 0; j < n; j++) {
                if (j != i) {
                    x[k++] = data[j];
                }
            }

            double[][] xT = new Array2DRowRealMatrix(x).transpose().getData();

            correlationCoefficients.add(MathUtil.createLinearRegression(xT,
                data[i]).calculateRSquared());
        }
        return correlationCoefficients;
    }

    public VmSelectionPolicy getFallbackPolicy() {
        return fallbackPolicy;
    }

    public void setFallbackPolicy(final VmSelectionPolicy fallbackPolicy) {
        this.fallbackPolicy = fallbackPolicy;
    }
}

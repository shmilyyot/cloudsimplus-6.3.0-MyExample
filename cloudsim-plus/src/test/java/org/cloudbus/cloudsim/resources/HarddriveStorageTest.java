package org.cloudbus.cloudsim.resources;

import org.apache.commons.lang3.StringUtils;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import org.cloudbus.cloudsim.distributions.ExponentialDistr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author Manoel Campos da Silva Filho
 */
public class HarddriveStorageTest {
    private static final int CAPACITY = 1000;

    @Test()
    public void testNewHarddriveStorageWhenOnlyWhiteSpacesName() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HarddriveStorage("   ", CAPACITY));
    }

    @Test()
    public void testNewHarddriveStorageWhenEmptyName() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HarddriveStorage("", CAPACITY));
    }

    @Test()
    public void testNewHarddriveStorageWheNullName() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HarddriveStorage(null, CAPACITY));
    }

    @Test()
    public void testNewHarddriveStorageWhenNegativeSize() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HarddriveStorage(-1));
    }

    @Test
    public void testNewHarddriveStorageWhenZeroSize() {
        final int expResult = 0;
        final HarddriveStorage hd = new HarddriveStorage(expResult);
        assertEquals(expResult, hd.getCapacity());
    }

    @Test
    public void testGetCapacity() {
        final HarddriveStorage instance = createHardDrive(CAPACITY);
        assertEquals(CAPACITY, instance.getCapacity());
    }

    @Test
    public void testGetTransferTime() {
        final HarddriveStorage instance = createHardDrive(1);
        final int fileSizeInMB = 100;
        final int maxTransferRateInMbitsSec = 10;
        final int latencyInSec = 1;
        final int expectedSecs = 81;
        instance.setLatency(latencyInSec);
        instance.setMaxTransferRate(maxTransferRateInMbitsSec);

        assertEquals(expectedSecs, instance.getTransferTime(fileSizeInMB));
    }

    @Test
    public void testGetName() {
        final String expResult = "hd1";
        final HarddriveStorage instance = createHardDrive(CAPACITY, expResult);
        assertEquals(expResult, instance.getName());
    }

    @Test()
    public void testSetLatencyNegative() {
        final HarddriveStorage instance = createHardDrive();
        Assertions.assertThrows(IllegalArgumentException.class, () -> instance.setLatency(-1));
    }

    @Test
    public void testSetLatency0() {
        final HarddriveStorage instance = createHardDrive();
        final int expected = 0;
        instance.setLatency(expected);
        assertEquals(expected, instance.getLatency());
    }

    @Test
    public void testSetLatency1() {
        final HarddriveStorage instance = createHardDrive();
        final double latency = 1;
        instance.setLatency(latency);
        assertEquals(latency, instance.getLatency());
    }

    @Test
    public void testSetMaxTransferRate1() {
        final HarddriveStorage instance = createHardDrive();
        final int rate = 1;
        instance.setMaxTransferRate(rate);
        assertEquals(rate, instance.getMaxTransferRate());
    }

    @Test()
    public void testSetMaxTransferRateNegative() {
        final HarddriveStorage instance = createHardDrive();
        Assertions.assertThrows(IllegalArgumentException.class, () -> instance.setMaxTransferRate(-1));
    }

    @Test()
    public void testSetMaxTransferRate0() {
        final HarddriveStorage instance = createHardDrive();
        Assertions.assertThrows(IllegalArgumentException.class, () -> instance.setMaxTransferRate(0));
    }

    @Test
    public void testSetAvgSeekTimeWhenDouble() {
        testSetAvgSeekTime(null);
    }

    @Test
    public void testSetAvgSeekTimeWhenDoubleContinuousDistribution() {
        final double anyValue = 2.4;
        testSetAvgSeekTime(new ExponentialDistr(anyValue));
    }

    /**
     * Private method called by the overloaded versions of the
     * setAvgSeekTime method.
     * @param gen A random number generator. The parameter can be
     * null in order to use the simpler version of the setAvgSeekTime.
     */
    private void testSetAvgSeekTime(final ContinuousDistribution gen) {
        final HarddriveStorage instance = createHardDrive();
        final double seekTime = 1;
        setAvgSeekTime(instance, seekTime, gen);
        assertAll(
            () -> assertEquals(seekTime, instance.getAvgSeekTime()),
            () -> assertEquals(seekTime, instance.getAvgSeekTime()),
            () -> assertThrows(IllegalArgumentException.class, () -> setAvgSeekTime(instance, -1, gen)),
            () -> assertEquals(seekTime, instance.getAvgSeekTime())
        );
    }

    private static FileStorage setAvgSeekTime(
            final HarddriveStorage instance, final double seekTime,
            final ContinuousDistribution gen) {
        if(gen != null) {
            return instance.setAvgSeekTime(seekTime, gen);
        }

        return instance.setAvgSeekTime(seekTime);
    }

    /**
     * Creates a hard drive with the {@link #CAPACITY} capacity.
     * @return
     */
    private HarddriveStorage createHardDrive() {
        return createHardDrive(CAPACITY);
    }

    private HarddriveStorage createHardDrive(final long capacity) {
        return createHardDrive(capacity, "");
    }

    private HarddriveStorage createHardDrive(final long capacity, final String name) {
        if(StringUtils.isBlank(name)) {
            return new HarddriveStorage(capacity);
        }

        return new HarddriveStorage(name, capacity);
    }
}

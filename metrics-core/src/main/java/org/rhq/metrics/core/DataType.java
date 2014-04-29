package org.rhq.metrics.core;

/**
 * @author John Sanda
 */
public enum DataType {

    RAW, MAX, MIN, AVG;

    public static DataType valueOf(int type) {
        switch (type) {
        case 0:  return RAW;
        case 1 : return MAX;
        case 2 : return MIN;
        case 3 : return AVG;
        default: throw new IllegalArgumentException(type + " is not a supported " +
            DataType.class.getSimpleName());
        }
    }

}

package org.rhq.metrics.util;

import java.util.Date;
import java.util.UUID;

import com.eaio.uuid.UUIDGen;

import org.joda.time.DateTime;

/**
 * @author John Sanda
 */
public class TimeUUIDUtils {

    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

    // This function is taken from the Hector library.
    //
    //    The MIT License
    //
    //            Copyright (c) 2010 Ran Tavory
    //
    //    Permission is hereby granted, free of charge, to any person obtaining a copy
    //    of this software and associated documentation files (the "Software"), to deal
    //    in the Software without restriction, including without limitation the rights
    //    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    //    copies of the Software, and to permit persons to whom the Software is
    //    furnished to do so, subject to the following conditions:
    //
    //    The above copyright notice and this permission notice shall be included in
    //    all copies or substantial portions of the Software.
    //
    //    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    //    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    //    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    //    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    //    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    //    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    //    THE SOFTWARE.
    public static UUID getTimeUUID(long time) {
        return new UUID(createTime(time), UUIDGen.getClockSeqAndNode());
    }

    private static long createTime(long currentTime) {
        long time;

        // UTC time
        long timeToUse = (currentTime * 10000) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;

        // time low
        time = timeToUse << 32;

        // time mid
        time |= (timeToUse & 0xFFFF00000000L) >> 16;

        // time hi and version
        time |= 0x1000 | ((timeToUse >> 48) & 0x0FFF); // version 1
        return time;
    }

    public static UUID getTimeUUID(Date d) {
        return getTimeUUID(d.getTime());
    }

    public static UUID getTimeUUID(DateTime d) {
        return getTimeUUID(d.getMillis());
    }

}

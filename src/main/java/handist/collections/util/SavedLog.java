package handist.collections.util;

import static apgas.Constructs.*;
import static handist.collections.util.StringUtilities.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import apgas.Constructs;
import apgas.Place;
import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.dist.DistCol;
import handist.collections.dist.DistLog;
import handist.collections.dist.DistLog.LogItem;
import handist.collections.dist.DistLog.LogKey;
import handist.collections.dist.DistMultiMap;
import handist.collections.glb.GlobalLoadBalancer;

/**
 * Class used as a substitute to {@link DistLog} to save the events recorded
 * into a DistLog to a file and to restore it later.
 * <p>
 * This class is used so that distributed logs can be accessed for processing
 * and analysis post-mortem in single-threaded environments, something not
 * possible when working directly with a {@link DistLog}.
 *
 * @author Patrick Finnerty
 *
 */
public class SavedLog {

    /**
     * Class used as a substitute to {@link LogKey} in which the use of the APGAS
     * {@link Place} class has been replaced by a {@code int}.
     *
     * @author Patrick Finnerty
     *
     */
    public static class Key implements Serializable {

        /** Serial Version UID */
        private static final long serialVersionUID = 6245573767313436511L;

        /** Phase during which the events were recorded */
        public final long phase;
        /** Place on which the events occurred */
        public final int place;
        /** Tag under which the events are kept */
        public final String tag;

        /**
         * Constructor
         *
         * @param p place number ({@link Place#id}) on which the events occurred
         * @param t tag under which the events are gathered
         * @param f the phase during which the events occurred
         */
        private Key(int p, String t, long f) {
            place = p;
            tag = t;
            phase = f;
        }

        /**
         * Two {@link Key}s are equal iff their respective {@link #place}, {@link #tag}
         * and {@link #phase} match. Note that {@link Key} can be compared to a
         * {@link LogKey}.
         */
        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            } else if (obj instanceof LogKey) {
                final LogKey logKey = (LogKey) obj;
                return place == logKey.place.id && nullSafeEquals(tag, logKey.tag) && (phase == logKey.phase);
            } else if (obj instanceof Key) {
                final Key key2 = (Key) obj;
                return place == key2.place && nullSafeEquals(tag, key2.tag) && (phase == key2.phase);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return place + (tag.hashCode() << 2) + (int) (phase << 4 + phase >> 16);
        }

        @Override
        public String toString() {
            return "Log@Place(" + place + "), tag: " + tag + ", phase: " + phase;
        }

    }

    /**
     * Main used to generate a file into which the events of GLB program execution
     * are recorded
     *
     * @param args one argument: the name of the file into which the log will be
     *             stored
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("1 arguments required:");
            System.err.println("\t<file name> \tspecifies the file into which the log recording will be written");
            return;
        }

        final DistCol<Long> col = new DistCol<>();
        for (long l = 0l; l < 1000; l += 100) {
            final Chunk<Long> c = new Chunk<>(new LongRange(l, l + 100), idx -> {
                return new Long(idx);
            });
            col.add(c);
        }

        GlobalLoadBalancer.underGLB(() -> {
            col.GLB.forEach(i -> i.compareTo(i + 42));
        });

        final DistLog distLog = GlobalLoadBalancer.getPreviousLog();
        distLog.globalGather();
        distLog.printAll(System.out);
        final SavedLog log = new SavedLog(distLog);
        try {
            log.saveToFile(new File(args[0]));
        } catch (final IOException e) {
            System.err.println("Issue when trying to write to file:");
            e.printStackTrace();
        }
    }

    /**
     * Map into which the logged entries of the {@link DistLog} are converted
     */
    private final HashMap<Key, Collection<LogItem>> loggedEntries;

    private final int numberOfHosts;

    /**
     * Constructor
     *
     * @param log the distributed log instance into which events that occurred
     *            during a GLB execution were recorded
     */
    public SavedLog(DistLog log) {
        numberOfHosts = Constructs.places().size();

        log.globalGather();

        final DistMultiMap<LogKey, LogItem> distLogMap = log.getDistMultiMap();
        loggedEntries = new HashMap<>(distLogMap.size());

        // Initialize member loggedEntries by substituting the keys used to log the
        // various entries
        log.getDistMultiMap().forEach((key, entries) -> {
            final Key substituteKey = new Key(key.place.id, key.tag, key.phase);
            loggedEntries.put(substituteKey, entries);
        });
    }

    /**
     * Constructor
     * <p>
     * This constructor
     *
     * @param file the file to which an instance of this class was saved
     * @throws IOException            if thrown during the retrieval of information
     *                                from the specified file
     * @throws ClassNotFoundException if thrown when reading objects from the
     *                                specified file
     */
    @SuppressWarnings("unchecked")
    public SavedLog(File file) throws IOException, ClassNotFoundException {
        final ObjectInputStream inStream = new ObjectInputStream(new FileInputStream(file));
        numberOfHosts = inStream.readInt();
        loggedEntries = (HashMap<Key, Collection<LogItem>>) inStream.readObject();
        inStream.close();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof SavedLog) {
            return equalsGlbLog((SavedLog) o);
        } else if (o instanceof DistLog) {
            return equalsDistLog((DistLog) o);
        } else {
            return false;
        }
    }

    private boolean equalsDistLog(DistLog log) {
        final Map<LogKey, Collection<LogItem>> logMap = log.getDistMultiMap();
        if (logMap.size() != loggedEntries.size()) {
            return false;
        }

        for (final Key k : loggedEntries.keySet()) {
            final Collection<LogItem> otherItems = log.getLog(place(k.place), k.tag, k.phase);
            final Collection<LogItem> myItems = loggedEntries.get(k);

            if (otherItems != null && otherItems.size() == myItems.size()) {
                // Check that every item in 'myItems' is also in 'otherItems'
                if (!otherItems.containsAll(myItems)) {
                    return false;
                }
            } else {
                return false;
            }
            // 2-way comparison
        }

        return true;
    }

    /**
     * Checks if the provided GlbLog contains the same
     *
     * @param log
     * @return
     */
    private boolean equalsGlbLog(SavedLog log) {
        final Map<Key, Collection<LogItem>> logMap = log.loggedEntries;
        if (logMap.size() != loggedEntries.size()) {
            return false;
        }

        for (final Key k : loggedEntries.keySet()) {
            final Collection<LogItem> otherItems = log.getLog(k.place, k.tag, k.phase);
            final Collection<LogItem> myItems = loggedEntries.get(k);
            System.out.println(otherItems.size() + " " + myItems.size());

            if (otherItems != null && otherItems.size() == myItems.size()) {
                // Check that every item in 'myItems' is also in 'otherItems'
                if (!otherItems.containsAll(myItems)) {
                    return false;
                }
            } else {
                return false;
            }
            // 2-way comparison
        }

        return true;
    }

    /**
     * Obtain the logged entries for the specified place, tag, and phase tuple.
     *
     * @param place the number id of the place from which events should be retrieved
     * @param tag   the tag under which the logged items were gathered
     * @param phase the phase during which the events were logged
     * @return a collection containing the {@link LogItem} that were recorded under
     *         the specified tuple, {@code null} if there are no such
     */
    public Collection<LogItem> getLog(int place, String tag, long phase) {
        return getLog(new Key(place, tag, phase));
    }

    /**
     * Obtain the logged entries for the specified key
     *
     * @param k the key for which logged elements should be retrieved
     * @return collection of {@link LogItem} matching the key, or {@code null} if
     *         there are no such elements
     */
    public Collection<LogItem> getLog(Key k) {
        return loggedEntries.get(k);
    }

    /**
     * Obtain the logged entries for the specified log key. The provided key is
     * converted from {@link LogKey} to {@link Key} to retrieve the logged elements
     * from the {@link SavedLog} object.
     *
     * @param k key from a {@link DistLog} instance
     * @return collection of logged items that
     */
    public Collection<LogItem> getLog(LogKey k) {
        return getLog(k.place.id, k.tag, k.phase);
    }

    /**
     * Returns the number of hosts that took part in the execution this
     * {@link SavedLog} is the
     *
     * @return the number of processes involved in this computation
     */
    public int placeCount() {
        return numberOfHosts;
    }

    public void printAll(PrintStream out) {
        // Custom map sorted by place first and tag second
        final TreeMap<Key, Collection<LogItem>> sorted = new TreeMap<>((o1, o2) -> {
            int result = Integer.compareUnsigned(o1.place, o2.place);
            if (result == 0) {
                result = o1.tag.compareTo(o2.tag);
            }
            if (result == 0) {
                result = Long.compare(o1.phase, o2.phase);
            }
            return result;
        });

        // Insert all logs into the map so that they get sorted
        for (final Entry<Key, Collection<LogItem>> entry : loggedEntries.entrySet()) {
            sorted.put(entry.getKey(), entry.getValue());
        }

        // Traverse the sorted map and print each log on a dedicated line
        sorted.forEach((Key key, Collection<LogItem> items) -> {
            out.println("LogKey: " + key);
            for (final LogItem item : items) {
                out.println(item);
            }
        });

    }

    /**
     * Records this instance to a file for later retrieval
     *
     * @param file the file to which this instance needs to be saved to
     * @throws IOException if thrown during the process of saving this class to the
     *                     specified file
     */
    public void saveToFile(File file) throws IOException {
        final ObjectOutputStream outStream = new ObjectOutputStream(new FileOutputStream(file));

        outStream.writeInt(numberOfHosts);
        outStream.writeObject(loggedEntries);

        outStream.flush();
        outStream.close();
    }
}

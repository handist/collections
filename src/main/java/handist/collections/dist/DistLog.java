package handist.collections.dist;

import static apgas.Constructs.*;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import apgas.Constructs;
import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.dist.util.Pair;
import handist.collections.dist.util.SerializableBiFunction;

/**
 * DistLog is a distributed log manager. It collects log on places and gather it
 * to a place.
 *
 * Each DistLog instance refers to a distributed collection having global ID and
 * you can arbitrary copy it to other places.
 *
 * It offers a static method {@code DistLog.log(tag, msg)}. Please call
 * `DistLog.globalSetup(DistLog)` first, otherwise the method only print the
 * {@code tag} and {@code msg} to {@code System.out}.
 *
 */
public class DistLog extends DistCollectionSatellite<DistConcurrentMultiMap<DistLog.LogKey, DistLog.LogItem>, DistLog>
        implements Serializable {

    static class ListDiff {
        int index;
        LogItem first;
        LogItem second;

        public ListDiff(int index, LogItem first, LogItem second) {
            this.index = index;
            this.first = first;
            this.second = second;
        }

        @Override
        public String toString() {
            return "Elements (No. " + index + ") differ: " + first + ", " + second;
        }
    }

    public static class LogItem implements Serializable {
        private static final long serialVersionUID = -1365865614858381506L;
        public static boolean appendixPrint = true;
        public static Comparator<LogItem> cmp = Comparator.comparing(i0 -> i0.msg);
        public final String msg;
        public final String appendix;

        LogItem(Object body, Object app) {
            msg = (body != null ? body.toString() : "");
            appendix = (app != null ? app.toString() : null);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof LogItem)) {
                return false;
            }
            return msg.equals(((LogItem) obj).msg);
        }

        @Override
        public int hashCode() {
            return msg.hashCode();
        }

        @Override
        public String toString() {

            if(appendixPrint)
                return "LogItem:" + msg + ", " + appendix;
            else
                return "LogItem:" + msg;
        }
    }

    public static final class LogKey implements Serializable,Comparable<LogKey> {

        /** Serial Version UID */
        private static final long serialVersionUID = -7799219001690238705L;

        public final Place place;
        public final String tag;
        public final long phase;

        public LogKey(Place p, String tag, long phase) {
            this.place = p;
            this.tag = tag;
            this.phase = phase;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof LogKey)) {
                return false;
            }
            final LogKey key2 = (LogKey) obj;
            return place.equals(key2.place) && strEq(tag, key2.tag) && (phase == key2.phase);
        }

        @Override
        public int hashCode() {
            return place.id + (tag.hashCode() << 2) + (int) (phase << 4 + phase >> 16);
        }

        boolean strEq(String s1, String s2) {
            if (s1 == null) {
                return s2 == null;
            }
            return s1.equals(s2);
        }

        @Override
        public String toString() {
            return "Log@" + place + ", tag: " + tag + ", phase: " + phase;
        }

        @Override
        public int compareTo(LogKey o) {
            int result = Long.compare(phase, o.phase);
            if(result==0) result = tag.compareTo(o.tag);
            if(result==0) result = Integer.compare(place.id, o.place.id);
            return result;
        }
    }

    /**
     *
     */

    static class SetDiff {
        Collection<LogItem> first;
        Collection<LogItem> second;

        public SetDiff(Collection<LogItem> first, Collection<LogItem> second) {
            this.first = first;
            this.second = second;
        }
        public boolean isEmpty() {
            return (first==null || first.isEmpty()) && (second==null ||second.isEmpty());
        }

        public void print(PrintStream out) {
            out.println("  1st:" + (first==null? "": first));
            out.println("  2nd:" + (second==null? "": second));
        }
    }

    /** Serial Version UID */
    private static final long serialVersionUID = 3453720633747873404L;

    /**
     * The default DistLog instance used by {@code DistLog.log} method.
     */
    public static DistLog defaultLog;

    public static HashMap<GlobalID, DistLog> map = new HashMap<>();

    private static <E> Collection<E> concat(Collection<? extends Collection<E>> lists) {
        if (lists == null) {
            return null;
        }
        int size = 0;
        for (final Collection<E> list : lists) {
            size += list.size();
        }
        final ArrayList<E> result = new ArrayList<>(size);
        for (final Collection<E> list : lists) {
            result.addAll(list);
        }
        return result;
    }

    public static void defaultGlobalGather() {
        DistLog.defaultLog.globalGather();
    }

    /**
     * set the phase of DistLog.defaultLog
     *
     * @param phase the new phase value
     */
    public static void defaultGlobalSetPhase(long phase) {
        DistLog.defaultLog.globalSetPhase(phase);
    }

    private static ListDiff diffCheckList(List<LogItem> list0, List<LogItem> list1) {
        final int size = Math.min(list0.size(), list1.size());
        for (int i = 0; i < size; i++) {
            final LogItem item0 = list0.get(i);
            final LogItem item1 = list1.get(i);
            if (!item0.equals(item1)) {
                return new ListDiff(i, item0, item1);
            }
        }
        if (list0.size() > list1.size()) {
            return new ListDiff(size, list0.get(size), null);
        } else if (list0.size() < list1.size()) {
            return new ListDiff(size, null, list1.get(size));
        } else {
            return null;
        }
    }

    private static SetDiff diffCheckSet0(Collection<LogItem> olist0, Collection<LogItem> olist1) {
        if(olist0.isEmpty()&&olist1.isEmpty()) return null;
        if(olist0.isEmpty() || olist1.isEmpty()) {
            return new SetDiff(olist0, olist1);
        }
        final ArrayList<LogItem> list0 = new ArrayList<>(olist0);
        final ArrayList<LogItem> list1 = new ArrayList<>(olist1);
        list0.sort(LogItem.cmp);
        list1.sort(LogItem.cmp);
        SetDiff result = new SetDiff(list0, list1);
        if(list0.size()!=list1.size()) return result;
        for(int i=0; i< list0.size(); i++) {
            if(!list0.get(i).equals(list1.get(i))) return result;
        }
        return null;
    }

    private static SetDiff diffCheckSet(Collection<? extends Collection<LogItem>> lists0,
            Collection<? extends Collection<LogItem>> lists1) {
        if (lists0 == null) {
            lists0 = Collections.emptySet();
        }
        if (lists1 == null) {
            lists1 = Collections.emptySet();
        }
        return diffCheckSet0(concat(lists0), concat(lists1));
    }

    public DistConcurrentMultiMap<LogKey, LogItem> getDistMultiMap() {
        return getPlanet();
    }

    /**
     * create a DistLog instance and set it to {@code DistLog.defaultLog} on all the
     * places of the place group.
     *
     * @param pg         the place group
     * @param phase      the initial phase of the created DistLog
     * @param setDefault set the created DistLog as DistLog.defaultLog
     */
    public static DistLog globalSetup(TeamedPlaceGroup pg, long phase, boolean setDefault) {
        final DistLog dlog = new DistLog(pg, phase);
        dlog.globalSetup(setDefault);
        return dlog;
    }

    /**
     * put a log to {@code DistLog.defaultLog}. This method only calls
     * {@code DistLog.defaultLog.put(tag, msg, appendix)} if {@code defaultLog} is
     * set. Otherwise {@code System.out.println()} is called instead.
     *
     * @param tag      the topic tag about which a log is made
     * @param msg      object being logged, can be a {@link String} or another
     *                 object
     * @param appendix appendix of the log message
     */
    public static void log(String tag, Object msg, Object appendix) {
        if (defaultLog != null) {
            defaultLog.put(tag, msg, appendix);
        } else {
            System.out.println(tag + ":" + msg + (appendix == null ? "" : ":" + appendix));
        }
    }

    public final TeamedPlaceGroup pg;

    /** The current logging phase */
    AtomicLong phase;

    /**
     * create a DistLog instance that records logs within a place group and gather
     * them to a place
     *
     * @param pg    the place group
     * @param phase the initial phase
     */
    public DistLog(final TeamedPlaceGroup pg, final long phase) {
        this(pg, phase, new DistConcurrentMultiMap<>(pg));
    }

    DistLog(TeamedPlaceGroup pg, long phase, DistConcurrentMultiMap<LogKey, LogItem> base) {
        super(base);
        this.pg = pg;
        this.phase = new AtomicLong(phase);
        assert (DistLog.map.get(base.id()) == null);
        assert (base != null);
        DistLog.map.put(base.id(), this);
    }

    /**
     * Determines whether the log set of this and the target is equals. It
     * distinguish only tags and ignores the generated places. It return true iff
     * the sets of the logs having the same tag are the same. Diff will be output to
     * {@code out} if they differs.
     *
     * @param target the logger instance to compare to this
     * @param out    the output to which the difference is printed
     * @return {@code true} if this instance and the target are identical,
     *         {@code false otherwise}
     */

    public boolean distributionFreeEquals(DistLog target, PrintStream out) {
        boolean result = true;
        final TreeMap<Pair<String, Long>, List<Collection<LogItem>>> g0 = groupBy();
        final TreeMap<Pair<String, Long>, List<Collection<LogItem>>> g1 = target.groupBy();
        TreeSet<Pair<String, Long>> keys = new TreeSet<>(gcmp);
        keys.addAll(g0.keySet());
        keys.addAll(g1.keySet());
        long phase = 0;
        for (Pair<String, Long> key: keys) {
            if((!result) && phase < key.second) return false;
            final List<Collection<LogItem>> entries0 = g0.get(key);
            final List<Collection<LogItem>> entries1 = g1.get(key);
            final SetDiff diff = diffCheckSet(entries0, entries1);
            if (diff != null) {
                if(result==true) out.println("Diff first found in phase " + key.second);
                out.println("Diff with tag: " + key.first + ", phase: " + key.second);
                diff.print(out);
                result = false;
                phase = key.second;
            }
        }
        return result;
    }

    private static final Comparator<Pair<String,Long>> gcmp =
            Comparator.comparing(Pair<String,Long>::getSecond).thenComparing(Pair<String,Long>::getFirst);


    @Override
    public SerializableBiFunction<DistConcurrentMultiMap<LogKey, LogItem>, Place, DistLog> getBranchCreator() {
        final DistConcurrentMultiMap<LogKey, LogItem> base0 = base;
        final long phase0 = phase.get();
        return (DistConcurrentMultiMap<LogKey, LogItem> base, Place place) -> new DistLog(base0.placeGroup(), phase0,
                base0);
    }

    public Collection<LogItem> getLog(String key) {
        return base.get(new LogKey(here(), key, phase.get()));
    }
    public Collection<LogItem> removeLog(String key) {
        return base.remove(new LogKey(here(), key, phase.get()));
    }

    public long getPhase() {
        return phase.get();
    }



    /**
     * gather the log to the receiving place initially specified
     */
    public void globalGather() {
        // TODO
        // base.GLOBAL.gather(this.place);
        final DistConcurrentMultiMap<LogKey, LogItem> base0 = base;
        final Place dest = here();
        pg.broadcastFlat(() -> {
            final Function<LogKey, Place> func = (LogKey k) -> dest;
            base0.relocate(func);
        });
    }

    public void globalSetDefault() {
        final DistLog b = this;
        DistLog.defaultLog = b;
        final Place caller = here();
        pg.broadcastFlat(() -> {
            if (!here().equals(caller)) {
                DistLog.defaultLog = b;
            }
        });
    }

    /**
     * set the phase of DistLog on each place
     *
     * @param phase the logging phase to be used from now on
     */
    public void globalSetPhase(final long phase) {
        if (base == null) {
            throw new IllegalStateException(
                    "Note: `DistLog#globalSetPhase` can be used after `globalSetup()` called. ");
        }
        final DistLog b = this;
        b.setPhase(phase);
        final Place caller = Constructs.here();
        pg.broadcastFlat(() -> {
            if (Constructs.here().equals(caller)) {
                return;
            }
            b.setPhase(phase);
        });
    }

    /**
     * set this instance to {@code DistLog.defaultLog} on all the places of the
     * place group.
     */
    public void globalSetup(final boolean setDefault) {
        if (setDefault) {
            globalSetDefault();
        }
    }

    private TreeMap<Pair<String, Long>, List<Collection<LogItem>>> groupBy() {
        final TreeMap<Pair<String, Long>, List<Collection<LogItem>>> results = new TreeMap<>(gcmp);
        base.forEach((LogKey key, Collection<LogItem> items) -> {
            final Pair<String, Long> keyWOp = new Pair<>(key.tag, key.phase);
            final List<Collection<LogItem>> bag = results.computeIfAbsent(keyWOp, k -> new ArrayList<>());
            bag.add(items);
        });
        return results;
    }

    /**
     * Determines whether the log set of this and the target is equals. It
     * distinguish the log having the different tags or different generated places.
     * It only returns true only iff the two log lists have the same elements with
     * the same order for each generated place and tag. Diff will be output to
     * {@code out} if they differs.
     *
     * @param target the target to which this instance needs to be compared
     * @param out    the output stream to which the difference will be printed
     * @return {@code true} if the target and this logs are equal,
     *         {@code flase otherwise}
     */
    public boolean placeConsciousEquals(DistLog target, PrintStream out, boolean asList) {
        if (target == null) {
            if (out != null) {
                out.println("DistLog differs: target is null");
            }
            return false;
        }

        TreeSet<LogKey> keys = new TreeSet<>();
        keys.addAll(base.keySet());
        keys.addAll(target.base.keySet());

        boolean result = true;
        long bugPhase = 0;

        for (final LogKey key: keys) {
            if((!result) && key.phase > bugPhase) return false;
            final Collection<LogItem> elems1 = base.get(key);
            final Collection<LogItem> elems2 = target.base.get(key);
            final List<LogItem> lists1 = elems1==null? Collections.EMPTY_LIST: new ArrayList<>(elems1);
            final List<LogItem> lists2 = elems2==null? Collections.EMPTY_LIST: new ArrayList<>(elems2);
            if (asList) {
                final ListDiff diff = diffCheckList(lists1, lists2);
                if (diff != null) {
                    if(result) {
                        out.println("Diff first found in phase " + key.phase);
                        bugPhase = key.phase; result = false;
                    }
                    out.println("Diff in " + key + "::" + diff);
                }
            } else {
                final SetDiff diff = diffCheckSet0(lists1, lists2);
                if (diff != null) {
                    if(result) {
                        out.println("Diff first found in phase " + key.phase);
                        bugPhase = key.phase; result = false;
                    }
                    out.println("Diff in " + key);
                    diff.print(out);
                }
            }
        }
        return result;
    }

    public static String itemOffset = "  ";
    public void printAll(PrintStream out) {
        final TreeMap<LogKey, Collection<LogItem>> sorted = new TreeMap<>((o1, o2) -> {
            int result = Integer.compareUnsigned(o1.place.id, o2.place.id);
            if (result == 0) {
                result = o1.tag.compareTo(o2.tag);
            }
            if (result == 0) {
                result = Long.compare(o1.phase, o2.phase);
            }
            return result;
        });
        base.forEach((LogKey key, Collection<LogItem> items) -> {
            sorted.put(key, items);
        });
        sorted.forEach((LogKey key, Collection<LogItem> items) -> {
            out.println("LogKey: " + key);
            for (final LogItem item : items) {
                out.println(itemOffset + item);
            }
        });

    }

    /**
     * put the msg with the specified tag.
     *
     * @param tag      tag for the message.
     * @param msg      the message to be stored. The value of {@code msg.toString()}
     *                 is stored into DistLog.
     * @param appendix the appendix information for the log. The value of appendix
     *                 is not used for equality check of {@code DistLog} instances
     *                 while {@code msg} is used.
     */
    public void put(String tag, Object msg, Object appendix) {
        base.put1(new LogKey(Constructs.here(), tag, phase.get()), new LogItem(msg, appendix));
    }

    public void put(long phaseVal, String tag, Object msg, Object appendix) {
        base.put1(new LogKey(Constructs.here(), tag, phaseVal), new LogItem(msg, appendix));
    }

    public void setPhase(long phase) {
        this.phase.set(phase);
    }
}

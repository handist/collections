package handist.collections.dist;

import java.io.ObjectInputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import apgas.Constructs;
import apgas.Place;
import apgas.util.GlobalID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import handist.collections.dist.util.Pair;

import static apgas.Constructs.here;

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
public class DistLog implements Serializable, KryoSerializable {

     static class LogItem implements Serializable {
         private static final long serialVersionUID = -1365865614858381506L;
         public static Comparator<LogItem> cmp = Comparator.comparing(i0 -> i0.msg);
         String msg;
         String appendix;

         LogItem(Object body, Object app) {
            msg = (body != null ? body.toString() : "");
            appendix = (app != null ? app.toString() : null);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof LogItem)) {
                return false;
            }
            return this.msg.equals(((LogItem) obj).msg);
        }

        @Override
        public int hashCode() {
            return this.msg.hashCode();
        }

        @Override
        public String toString() {
            return "LogItem:" + msg + " : " + appendix;
        }
    }

    static final class LogKey implements Serializable {

        /** Serial Version UID */
        private static final long serialVersionUID = -7799219001690238705L;

        Place p;
        String tag;
        long phase;

        public LogKey(Place p, String tag, long phase) {
            this.p = p;
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
            return p.equals(key2.p) && strEq(tag, key2.tag) && (phase == key2.phase);
        }

        @Override
        public int hashCode() {
            return p.id + (tag.hashCode() << 2) + (int) (phase << 4 + phase >> 16);
        }

        boolean strEq(String s1, String s2) {
            if (s1 == null) {
                return s2 == null;
            }
            return s1.equals(s2);
        }

        @Override
        public String toString() {
            return "Log@" + p + ", tag: " + tag + ", phase: " + phase;
        }
    }

    /** Serial Version UID */
    private static final long serialVersionUID = 3453720633747873404L;

    /**
     * The default DistLog instance used by {@code DistLog.log} method.
     */
    public static DistLog defaultLog;

    public static HashMap<GlobalID, DistLog> map = new HashMap<>();

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

    TeamedPlaceGroup pg;

    /** The current logging phase */
    AtomicLong phase;

    DistConcurrentMultiMap<LogKey, LogItem> base;

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
        this.pg = pg;
        this.phase = new AtomicLong(phase);
        this.base = base;
        assert (DistLog.map.get(base.id()) == null);
        assert (base != null);
        DistLog.map.put(base.id(), this);
    }

    private void readObject(ObjectInputStream s) {
        try {
            // System.out.println("READRESOLVE CALLED:" +  Constructs.here() + ":this " +this + ", " + base.id() + ", " + DistLog.map.get(base.id()));
            s.defaultReadObject();
            this.phase = DistLog.map.get(base.id()).phase;
        } catch (Exception e) {
            e.printStackTrace();
        }
        //      return DistLog.map.get(base.id());
    }

    public void write(Kryo kryo, Output output) {
        // System.out.println("KRYOWrite CALLED:" +  Constructs.here() + ":this " +this + ", " + base.id() + ", " + DistLog.map.get(base.id()));
        kryo.writeClassAndObject(output, pg);
        kryo.writeClassAndObject(output, base);
    }
    public void read(Kryo kryo, Input input) {
        this.pg = (TeamedPlaceGroup) kryo.readClassAndObject(input);
        this.base =  (DistConcurrentMultiMap<LogKey, LogItem> ) kryo.readClassAndObject(input);
        this.phase = DistLog.map.get(base.id()).phase;
        // System.out.println("Kryoread CALLED:" +  Constructs.here() + ":this " +this + ", " + base.id() + ", " + DistLog.map.get(base.id()));
    }


    /**
     * Determines whether the log set of this and the target is equals. It
     * distinguish only tags and ignores the generated places. It return true iff
     * the sets of the logs having the same tag are the same. Diff will be output to
     * {@code out} if they differs.
     *
     * @param target  the logger instance to compare to this
     * @param out     the output to which the difference is printed
     * @return {@code true} if this instance and the target are identical,
     *         {@code false otherwise}
     */

    public boolean distributionFreeEquals(DistLog target, PrintStream out) {
        final Map<Pair<String, Long>, List<Collection<LogItem>>> g0 = groupBy();
        final Map<Pair<String, Long>, List<Collection<LogItem>>> g1 = target.groupBy();

        for (final Map.Entry<Pair<String, Long>, List<Collection<LogItem>>> entry : g0.entrySet()) {
            Pair<String, Long> key = entry.getKey();
            final List<Collection<LogItem>> entries0 = entry.getValue();
            final List<Collection<LogItem>> entries1 = g1.get(key);
            SetDiff<LogItem> diff = diffCheckSplitSet(entries0, entries1, LogItem.cmp);
            if(diff!=null) {
                out.println("Diff @ [tag: " + key.first + ", phase" + key.second + "]:" + diff);
                return false;
            }
        }
        return true;
    }

    /**
     * gather the log to the receiving place initially specified
     */
    public void globalGather() {
        // TODO
        // base.GLOBAL.gather(this.place);
        final DistConcurrentMultiMap<LogKey,LogItem> base0 = this.base;
        final Place dest = here();
        pg.broadcastFlat(()-> {
            Function<LogKey, Place> func = (LogKey k) -> dest;
            base0.relocate(func);
        });
    }

    public void globalSetDefault() {
        final GlobalID id = this.base.id();
        pg.broadcastFlat(() -> {
            DistLog.defaultLog = DistLog.map.get(id);
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
        final GlobalID id = base.id();
        pg.broadcastFlat(() -> {
            DistLog.map.get(id).setPhase(phase);
        });
    }

    /**
     * set this instance to {@code DistLog.defaultLog} on all the places of the
     * place group.
     */
    public void globalSetup(final boolean setDefault) {
        final long phase0 = this.phase.get();
        final DistConcurrentMultiMap<LogKey, LogItem> base0 = this.base;
        if (setDefault) {
            DistLog.defaultLog = this;
        }
        final Place caller = Constructs.here();
        pg.broadcastFlat(() -> {
            if(Constructs.here().equals(caller)) return;
            final DistLog branch = new DistLog(base0.placeGroup(), phase0, base0);
            if (setDefault) {
                DistLog.defaultLog = branch;
            }
        });
    }

    private Map<Pair<String, Long>, List<Collection<LogItem>>> groupBy() {
        final Map<Pair<String, Long>, List<Collection<LogItem>>> results = new HashMap<>();
        base.forEach((LogKey key, Collection<LogItem> items) -> {
            final Pair<String, Long> keyWOp = new Pair<>(key.tag, key.phase);
            List<Collection<LogItem>> bag = results.computeIfAbsent(keyWOp, k -> new ArrayList<>());
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

        for (final Map.Entry<LogKey, ? extends Collection<LogItem>> entry : base.entrySet()) {
            final LogKey key = entry.getKey();
            final Collection<LogItem> elems1 = entry.getValue();
            final Collection<LogItem> elems2 = target.base.get(key);
            ArrayList<LogItem> lists1 = new ArrayList<>(elems1);
            ArrayList<LogItem> lists2 = new ArrayList<>(elems2);
            if(asList) {
                ListDiff<LogItem> diff = diffCheckList(lists1, lists2);
                if (diff != null) {
                    out.println("Diff in " + key + "::" + diff);
                    return false;
                }
            } else {
                SetDiff<LogItem> diff = diffCheckSet(lists1, lists2, LogItem.cmp);
                if (diff != null) {
                    out.println("Diff in " + key + "::" + diff);
                    return false;
                }
            }
        }
        return true;
    }

    public void printAll(PrintStream out) {
        final TreeMap<LogKey, Collection<LogItem>> sorted = new TreeMap<>((o1, o2) -> {
            int result = Integer.compareUnsigned(o1.p.id, o2.p.id);
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
                out.println(item);
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

    private void setPhase(long phase) {
        this.phase.set(phase);
    }

    /**
     *
     */

    static class SetDiff<E> {
        E first;
        E second;
        public SetDiff(E first, E second) {
            this.first = first;
            this.second = second;
        }
        public String toString() {
            if (first != null) {
                return "" + first + " is only included in the first collection.";
            } else return "" + second + " is only included in the second collection.";
        }
    }
    static class ListDiff<E> {
        int index;
        E first;
        E second;

        public ListDiff(int index, E first, E second) {
            this.index = index;
            this.first = first;
            this.second = second;
        }
        public String toString() {
            return "Elements (No. "+ index + ") differ: " + first + ", " + second;
        }
    }

    public static <E> Collection<E> concat(Collection<? extends Collection<E>> lists) {
        if(lists == null) return null;
        int size = 0;
        for(Collection<E> list: lists) size += list.size();
        ArrayList<E> result = new ArrayList<>(size);
        for(Collection<E> list: lists) result.addAll(list);
        return result;
    }

    public static <E> SetDiff<E> diffCheckSplitSet(Collection<? extends Collection<E>> lists0, Collection<? extends Collection<E>> lists1,
                                         Comparator<E> comp) {
        if(lists0 == null) lists0 = Collections.emptySet();
        if(lists1 == null) lists1 = Collections.emptySet();
        return diffCheckSet(concat(lists0), concat(lists1), comp);
    }

    public static <E> SetDiff<E> diffCheckSet(Collection<E> olist0, Collection<E> olist1,  Comparator<E> comp) {
        ArrayList<E> list0 = new ArrayList<>(olist0);
        ArrayList<E> list1 = new ArrayList<>(olist1);
        list0.sort(comp);
        list1.sort(comp);
        int size = Math.min(list0.size(), list1.size());
        for(int i =0; i< size; i++){
            E item0 = list0.get(i);
            E item1 = list1.get(i);
            int result = comp.compare(item0, item1);
            if(result < 0) {
                return new SetDiff<>(item0, null);
            } else if (result > 0){
                return new SetDiff<>(null, item1);
            }
        }
        if(list0.size() > list1.size()) {
            return new SetDiff<>(list0.get(size), null);
        } else if (list0.size() < list1.size()) {
            return new SetDiff<>(null, list1.get(size));
        } else {
            return null;
        }
    }

    public static <E> ListDiff<E> diffCheckList(List<E> list0, List<E> list1) {
        int size = Math.min(list0.size(), list1.size());
        for(int i =0; i< size; i++){
            E item0 = list0.get(i);
            E item1 = list1.get(i);
            if(!item0.equals(item1))
                return new ListDiff<>(i, item0, item1);
        }
        if(list0.size() > list1.size()) {
            return new ListDiff<>(size, list0.get(size), null);
        } else if (list0.size() < list1.size()) {
            return new ListDiff<>(size, null, list1.get(size));
        } else {
            return null;
        }
    }

    public static void main(String[] args) {
        List<String> strs0 = Arrays.asList("abc", "bdd", "cde", "def");
        List<String> strs1 = Arrays.asList("bdd", "abc", "def", "cde");
        List<String> strs2 = Arrays.asList("bdd", "abc", "aaa" );

        System.out.println(diffCheckSet(strs0, strs1, String.CASE_INSENSITIVE_ORDER));
        System.out.println(diffCheckSet(strs0, strs2, String.CASE_INSENSITIVE_ORDER));
        Collection<Collection<String>> x =  Arrays.asList(strs0, strs1, strs2);
        Collection<Collection<String>> y =  Arrays.asList(strs2, strs0, strs1);
        System.out.println("" + x + "-> " + concat(x));
        System.out.println(diffCheckSplitSet(x, y, String.CASE_INSENSITIVE_ORDER));
    }
}

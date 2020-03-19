/* 要素を列挙できるタイプの Distribution, IterableDistribution などに改名すべ?　*/
/* 取り敢えずは、Long 専用で。本来 Range[K] { K <: Comparable} 対応にすべきかな？ */
package handist.util.dist;

import java.util.Map;

import apgas.Place;

interface RangedDistribution<R> {
    public Map<R, Place> placeRanges(R range);
}

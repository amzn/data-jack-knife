package com.amazon.djk.sort;

import java.io.IOException;
import java.util.List;

import com.amazon.djk.record.Record;

/**
 * 
 * http://developer.classpath.org/doc/java/util/Arrays-source.html
 *
 */
public class RecordSorter {
    private final FieldsComparator comparator;
    private int[] sortOrds = null;
    private static final int[] ONE_ORD = new int[]{0};

    public RecordSorter(SortSpec[] sortSpecs) {
        comparator = new FieldsComparator(sortSpecs);
    }

    /**
     * @param recordList2 
     * 
     */
    
    /**
     * 
     * @param recordList to be sorted
     * @return list of sorted ordinal numbers into the list
     * @throws IOException 
     */
    public int[] sort(List<Record> recordList) throws IOException {
        if (recordList.size() < 2) return ONE_ORD; // nothing to sort

        comparator.init(recordList.size());
        for (Record rec : recordList) {
            comparator.addRecord(rec);
        }
        
        sortOrds = comparator.getOrds();
        innerSort(sortOrds, recordList.size());
        
        return sortOrds;
    }
    
    /**
     * Performs a stable sort on the elements, arranging them according to their
     * natural order.
     *
     * @param a the int array to sort
     */
    private void innerSort(int[] array, int count) {
      qsort(array, 0, count);
    }
  
    /**
     * Finds the index of the median of three array elements.
     *
     * @param a the first index
     * @param b the second index
     * @param c the third index
     * @param d the array
     * @return the index (a, b, or c) which has the middle value of the three
     */
    private int med3(int a, int b, int c) {
        return (comparator.compare(a,b) < 0
           ? (comparator.compare(b,c) < 0 ? b : comparator.compare(a,c) < 0 ? c : a)
           : (comparator.compare(b,c) > 0 ? b : comparator.compare(a,c) > 0 ? c : a));

    }
  
    /**
     * Swaps the elements at two locations of an array
     *
     * @param i the first index
     * @param j the second index
     * @param a the array
     */
    private static void swap(int i, int j, int[] a) {
      int c = a[i];
      a[i] = a[j];
      a[j] = c;
    }
  
    /**
     * Swaps two ranges of an array.
     *
     * @param i the first range start
     * @param j the second range start
     * @param n the element count
     * @param a the array
     */
    private static void vecswap(int i, int j, int n, int[] a) {
      for ( ; n > 0; i++, j++, n--)
        swap(i, j, a);
    }
  
    /**
     * Performs a recursive modified quicksort.
     *
     * @param array the array to sort
     * @param from the start index (inclusive)
     * @param count the number of elements to sort
     */
    private void qsort(int[] array, int from, int count) {
        // Use an insertion sort on small arrays.
        if (count <= 7) {
            for (int i = from + 1; i < from + count; i++) {
                // ORIG: for (int j = i; j > from && array[j - 1] > array[j]; j--)
                for (int j = i; (j > from) && (comparator.compare(j - 1, j) > 0); j--) {
                    swap(j, j - 1, array);
                }
            }
            
            return;
        }
  
        // Determine a good median element.
        int mid = count / 2;
        int lo = from;
        int hi = from + count - 1;
  
        if (count > 40) {
            // big arrays, pseudomedian of 9
            int s = count / 8;
            lo = med3(lo, lo + s, lo + 2 * s);
            mid = med3(mid - s, mid, mid + s);
            hi = med3(hi - 2 * s, hi - s, hi);
        }
        
        mid = med3(lo, mid, hi);
  
        int a, b, c, d;
        int comp;
  
        // Pull the median element out of the fray, and use it as a pivot.
        swap(from, mid, array);
        a = b = from;
        c = d = from + count - 1;
  
        // Repeatedly move b and c to each other, swapping elements so
        // that all elements before index b are less than the pivot, and all
        // elements after index c are greater than the pivot. a and b track
        // the elements equal to the pivot.
        while (true) {
            // ORIG: while (b <= c && (comp = compare(array[b], array[from])) <= 0)
            while (b <= c && (comp = comparator.compare(b, from)) <= 0) {
              if (comp == 0) {
                  swap(a, b, array);
                  a++;
              }
              b++;
            }

            // ORIG: while (c >= b && (comp = compare(array[c], array[from])) >= 0)
            while (c >= b && (comp = comparator.compare(c, from)) >= 0) {
              if (comp == 0) {
                  swap(c, d, array);
                  d--;
              }
              c--;
            }
            
            if (b > c)
                break;
          
            swap(b, c, array);
            b++;
            c--;
        }
  
        // Swap pivot(s) back in place, the recurse on left and right sections.
        hi = from + count;
        int span;
        span = Math.min(a - from, b - a);
        vecswap(from, b - span, span, array);
  
        span = Math.min(d - c, hi - d - 1);
        vecswap(b, hi - span, span, array);
  
        span = b - a;
        if (span > 1) {
            qsort(array, from, span);
        }
  
        span = d - c;
        if (span > 1) {
            qsort(array, hi - span, span);
        }
    }
}

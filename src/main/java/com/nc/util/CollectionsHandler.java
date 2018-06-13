package com.nc.util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class CollectionsHandler {

	private static class Partition<T> extends AbstractList<List<T>> {
		final List<T> list;
		final int size;

		Partition(List<T> list, int size) {
			this.list = Objects.requireNonNull(list);
			this.size = size;
		}

		@Override
		public List<T> get(int index) {
			final int listSize = size();
			if (index < 0 || index >= listSize) {
				throw new IndexOutOfBoundsException();
			}
			final int start = index * size;
			final int end = Math.min(start + size, list.size());
			return list.subList(start, end);
		}

		@Override
		public boolean isEmpty() {
			return list.isEmpty();
		}

		@Override
		public int size() {
			int result = list.size() / size;
			if (result * size != list.size()) {
				result++;
			}
			return result;
		}
	}

	private static class RandomAccessPartition<T> extends Partition<T> implements RandomAccess {
		RandomAccessPartition(List<T> list, int size) {
			super(list, size);
		}
	}

	static final class RandomAccessReverseList<T> extends ReverseList<T> implements RandomAccess {
		RandomAccessReverseList(List<T> forwardList) {
			super(forwardList);
		}
	}

	static class ReverseList<T> extends AbstractList<T> {
		private final List<T> forwardList;

		ReverseList(List<T> forwardList) {
			this.forwardList = Objects.requireNonNull(forwardList);
		}

		@Override
		public final void add(int index, T element) {
			forwardList.add(reversePosition(index), element);
		}

		@Override
		public final void clear() {
			forwardList.clear();
		}

		@Override
		public final T get(int index) {
			return forwardList.get(reverseIndex(index));
		}

		List<T> getForwardList() {
			return forwardList;
		}

		@Override
		public final Iterator<T> iterator() {
			return listIterator();
		}

		@Override
		public final ListIterator<T> listIterator(int index) {
			final int start = reversePosition(index);
			final ListIterator<T> forwardIterator = forwardList.listIterator(start);
			return new ListIterator<T>() {

				boolean canRemoveOrSet;

				@Override
				public final void add(T e) {
					forwardIterator.add(e);
					forwardIterator.previous();
					canRemoveOrSet = false;
				}

				final void checkState() {
					if (!canRemoveOrSet) {
						throw new IllegalStateException();
					}
				}

				@Override
				public final boolean hasNext() {
					return forwardIterator.hasPrevious();
				}

				@Override
				public final boolean hasPrevious() {
					return forwardIterator.hasNext();
				}

				@Override
				public final T next() {
					if (!hasNext()) {
						throw new NoSuchElementException();
					}
					canRemoveOrSet = true;
					return forwardIterator.previous();
				}

				@Override
				public final int nextIndex() {
					return reversePosition(forwardIterator.nextIndex());
				}

				@Override
				public final T previous() {
					if (!hasPrevious()) {
						throw new NoSuchElementException();
					}
					canRemoveOrSet = true;
					return forwardIterator.next();
				}

				@Override
				public final int previousIndex() {
					return nextIndex() - 1;
				}

				@Override
				public final void remove() {
					checkState();
					forwardIterator.remove();
					canRemoveOrSet = false;
				}

				@Override
				public final void set(T e) {
					checkState();
					forwardIterator.set(e);
				}
			};

		}

		@Override
		public final T remove(int index) {
			return forwardList.remove(reverseIndex(index));
		}

		@Override
		protected final void removeRange(int fromIndex, int toIndex) {
			subList(fromIndex, toIndex).clear();
		}

		private int reverseIndex(int index) {
			final int size = size();
			if (index < 0 || index >= size) {
				throw new IndexOutOfBoundsException();
			}
			return size - 1 - index;
		}

		private int reversePosition(int index) {
			final int size = size();
			if (index < 0 || index > size) {
				throw new IndexOutOfBoundsException();
			}
			return size - index;
		}

		@Override
		public final T set(int index, T element) {
			return forwardList.set(reverseIndex(index), element);
		}

		@Override
		public final int size() {
			return forwardList.size();
		}

		@Override
		public final List<T> subList(int fromIndex, int toIndex) {
			if (fromIndex < 0 || toIndex < fromIndex || toIndex > size()) {
				throw new IndexOutOfBoundsException();
			}
			return reverse(forwardList.subList(reversePosition(toIndex), reversePosition(fromIndex)));
		}
	}

	public static <T> Set<T> difference(final Set<? extends T> l, final Set<? extends T> r) {
		Objects.requireNonNull(l);
		Objects.requireNonNull(r);

		final HashSet<T> rv = new HashSet<T>(l);
		rv.removeAll(r);

		return rv;
	}

	/**
	 * Produces an empty enumeration of informed type
	 *
	 * @param <T>
	 *            Any type
	 * @return Empty enumeration
	 */
	public static <T> Enumeration<T> emptyEnumeration() {
		return enumerationFromCollection(Collections.<T> emptyList());
	}

	/**
	 * Converts informed collection into an enumeration
	 *
	 * @param <T>
	 *            Any type
	 * @param collection
	 *            Collection
	 * @return Enumeration
	 */
	public static <T> Enumeration<T> enumerationFromCollection(final Collection<T> collection) {
		if (collection == null) {
			return null;
		}

		return enumerationFromIterator(collection.iterator());
	}

	/**
	 * Converts informed iterator into an enumeration
	 *
	 * @param <T>
	 *            Any type
	 * @param iterator
	 *            Iterator
	 * @return Enumeration
	 */
	public static <T> Enumeration<T> enumerationFromIterator(final Iterator<T> iterator) {
		if (iterator == null) {
			return null;
		}

		return new Enumeration<T>() {
			@Override
			public boolean hasMoreElements() {
				return iterator.hasNext();
			}

			@Override
			public T nextElement() {
				return iterator.next();
			}
		};
	}

	/**
	 * Creates a new List only with elements found in all specified lists
	 *
	 * @param <T>
	 * @param allLists
	 *            - lists that will be verified to find common elements
	 * @return a new list only with elements found on all lists
	 */
	public static <T> List<T> findEqualElements(final List<List<T>> allLists) {
		if (allLists == null || allLists.size() == 0) {
			throw new IllegalArgumentException("There must be at least one list");
		}

		final List<T> newList = new ArrayList<T>();
		final List<T> firstList = allLists.get(0);

		for (final T object : firstList) {
			boolean existsInAllLists = true;
			for (int i = 1; i < allLists.size(); i++) {
				if (!allLists.get(i).contains(object)) {
					existsInAllLists = false;
				}
			}

			if (existsInAllLists) {
				newList.add(object);
			}
		}

		return newList;
	}

	/**
	 * Creates a new List only with elements found in all specified lists
	 *
	 * @param <T>
	 * @param allLists
	 *            - lists that will be verified to find common elements
	 * @return a new list only with elements found on all lists
	 */
	public static <T> List<T> findEqualElements(@SuppressWarnings("unchecked") final List<T>... allLists) {
		if (allLists == null || allLists.length == 0) {
			throw new IllegalArgumentException("There must be at least one list");
		}

		final List<T> newList = new ArrayList<T>();
		final List<T> firstList = allLists[0];

		for (final T object : firstList) {
			boolean existsInAllLists = true;
			for (int i = 1; i < allLists.length; i++) {
				if (!allLists[i].contains(object)) {
					existsInAllLists = false;
				}
			}

			if (existsInAllLists) {
				newList.add(object);
			}
		}

		return newList;
	}

	public static <T> T firstOrNull(final Collection<T> source) {
		if (Objects.requireNonNull(source).isEmpty()) {
			return null;
		}
		if (source instanceof List) {
			return ((List<T>) source).get(0);
		} else {
			return source.iterator().next();
		}
	}

	public static <T> T firstOrNull(final List<T> source) {
		if (Objects.requireNonNull(source).isEmpty()) {
			return null;
		}
		return source.get(0);
	}

	public static <T> T firstOrNull(Set<? extends T> set) {
		Iterator<? extends T> itr;
		return (itr = Objects.requireNonNull(set).iterator()).hasNext() ? itr.next() : null;
	}

	public static <T> Set<T> intersection(final Set<? extends T> l, final Set<? extends T> r) {
		Objects.requireNonNull(l);
		Objects.requireNonNull(r);

		final HashSet<T> rv = new HashSet<T>(l);
		rv.retainAll(r);

		return rv;
	}

	public static <T> Iterable<Iterable<T>> lazyPartition(Iterator<T> src, int size) {
		return () -> {
			return new Iterator<Iterable<T>>() {
				boolean ready;

				@Override
				public boolean hasNext() {
					if (!ready) {
						ready = src.hasNext();
					}
					return ready;
				}

				@Override
				public Iterable<T> next() {
					if (!ready) {
						throw new IllegalStateException();
					}

					ready = false;

					return () -> {
						return new Iterator<T>() {
							int count;
							boolean ready;

							@Override
							public boolean hasNext() {
								if (!ready) {
									ready = count < size && src.hasNext();
								}
								return ready;
							}

							@Override
							public T next() {
								if (!ready) {
									throw new IllegalStateException();
								}
								count++;
								ready = false;

								return src.next();
							}
						};
					};
				}
			};
		};
	}

	/**
	 * For sequential processing only!
	 *
	 * @param src
	 * @param size
	 * @return
	 */
	public static <T> Stream<Stream<T>> lazyPartition(Stream<T> src, int size) {
		final Iterator<T> itr = src.iterator();

		final Iterable<Iterable<T>> partition = lazyPartition(itr, size);

		return StreamSupport.stream(partition.spliterator(), false).map(s -> StreamSupport.stream(s.spliterator(), false));
	}

	/**
	 * Converts an enum class values to a list
	 *
	 * @param <T>
	 *            Any type
	 * @param enumClass
	 *            Enum class
	 * @return List
	 */
	public static <T extends Enum<T>> List<T> listFromEnum(final Class<T> enumClass) {
		return listFromEnum(enumClass, null);
	}

	/**
	 * Converts an enum class values to a list
	 *
	 * @param <T>
	 *            Any type
	 * @param enumClass
	 *            Enum class
	 * @param comparator
	 *            Comparator for sorting. If null is informed, list will be sorted using it´s
	 *            natural order
	 * @return List
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Enum<T>> List<T> listFromEnum(final Class<T> enumClass, final Comparator<T> comparator) {
		if (enumClass == null) {
			return null;
		}

		List<T> list = null;
		try {
			list = Arrays.asList((T[]) enumClass.getMethod("values").invoke(enumClass));
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}

		Collections.sort(list, comparator);

		return list;
	}

	/**
	 * Converts an enumeration to a list
	 *
	 * @param <T>
	 *            Any type
	 * @param enumeration
	 *            Enumeration
	 * @return List
	 */
	public static <T> List<T> listFromEnumeration(final Enumeration<T> enumeration) {
		return listFromEnumeration(enumeration, null);
	}

	/**
	 * Converts an enumeration to a list using the informed comparator for sorting
	 *
	 * @param <T>
	 *            Any type
	 * @param enumeration
	 *            Enumeration
	 * @param comparator
	 *            Comparator for sorting. If null is informed, list will be sorted using it´s
	 *            natural order
	 * @return List
	 */
	public static <T> List<T> listFromEnumeration(final Enumeration<T> enumeration, final Comparator<T> comparator) {
		if (enumeration == null) {
			return null;
		}

		final ArrayList<T> list = new ArrayList<T>();

		while (enumeration.hasMoreElements()) {
			list.add(enumeration.nextElement());
		}

		Collections.sort(list, comparator);

		return list;
	}

	/**
	 * Generates a map where collection items are map keys
	 *
	 * @param <T>
	 *            Any type
	 * @param collection
	 *            Collection
	 * @return Map A map view of the informed collection
	 */
	public static <T> Map<T, Object> mapFromCollection(final Collection<T> collection) {
		if (collection == null) {
			return null;
		}

		final HashMap<T, Object> map = new HashMap<T, Object>();

		final Object holder = new Object();
		for (final T item : collection) {
			map.put(item, holder);
		}

		return map;
	}

	public static <T> Set<T> newHashSet(Iterator<T> iterator) {
		Objects.requireNonNull(iterator);

		final HashSet<T> rv = new HashSet<>();

		while (iterator.hasNext()) {
			rv.add(iterator.next());
		}

		return rv;
	}

	public static <T> Iterable<List<T>> partition(Iterable<T> src, int size) {
		return src instanceof List ? partition((List<T>) src, size) : partition(src.iterator(), size);
	}

	public static <T> Iterable<List<T>> partition(Iterator<T> src, int size) {
		return () -> {
			return new Iterator<List<T>>() {

				@Override
				public boolean hasNext() {
					return src.hasNext();
				}

				@Override
				public List<T> next() {
					if (!hasNext()) {
						throw new NoSuchElementException();
					}
					// use serializable return type
					final List<T> rv = new ArrayList<>(size);
					int count = 0;
					do {
						rv.add(src.next());
					} while (++count < size && src.hasNext());

					return rv;
				}
			};
		};
	}

	public static <T> List<List<T>> partition(List<T> src, int size) {
		return src instanceof RandomAccess ? new RandomAccessPartition<T>(src, size) : new Partition<T>(src, size);
	}

	public static <T> Stream<List<T>> partition(Stream<T> src, int size) {
		final Iterator<T> itr = src.iterator();

		final Iterable<List<T>> partition = partition(itr, size);

		return StreamSupport.stream(partition.spliterator(), false);
	}

	public static <T> List<T> reverse(List<T> source) {
		return source instanceof RandomAccess ? new RandomAccessReverseList<>(source) : new ReverseList<>(source);
	}

	public static <T> Set<T> union(final Set<T> l, final Set<T> r) {
		Objects.requireNonNull(l);
		Objects.requireNonNull(r);

		final HashSet<T> rv = new HashSet<T>(l);
		rv.addAll(r);

		return rv;
	}

	/**
	 * Avoids instantiation using default constructor
	 */
	private CollectionsHandler() {
		super();
	}

	/**
	 * Sorts informed collection into a list object. If informed collection is a list, a different
	 * sorted one will be returned.
	 *
	 * @param <T>
	 *            Any type
	 * @param source
	 *            Source collection
	 * @return Sorted list
	 */
	public <T> List<T> sort(final Collection<T> source) {
		return sort(source, null);
	}

	/**
	 * Sorts informed collection into a list object. If informed collection is a list, a different
	 * sorted one will be returned.
	 *
	 * @param <T>
	 *            Any type
	 * @param source
	 *            Source collection
	 * @param comparator
	 *            Optional comparator. If null uses natural object sort order
	 * @return Sorted list
	 */
	public <T> List<T> sort(final Collection<T> source, final Comparator<T> comparator) {
		if (source == null) {
			return null;
		}

		if (source.isEmpty()) {
			return new LinkedList<T>();
		}

		final LinkedList<T> list = new LinkedList<T>(source);

		Collections.sort(list, comparator);

		return list;
	}

}
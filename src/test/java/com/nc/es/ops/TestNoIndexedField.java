package com.nc.es.ops;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.nc.es.api.ElasticSearchOperationException;
import com.nc.es.api.IElasticSearchOps;
import com.nc.es.api.IElasticSearchOps.ITypedBound;
import com.nc.es.config.IndexDefinition;
import com.nc.es.config.IndexedType;
import com.nc.es.config.Mappings;
import com.nc.es.config.Property;
import com.nc.es.config.Property.Type;
import com.nc.es.config.Settings;
import com.nc.es.ops.MappedSearchResult;
import com.nc.es.ops.Result;
import com.nc.es.ops.Result.Status;
import com.nc.es.search.ILeafQuery.Range;
import com.nc.es.search.ILeafQuery.Term;
import com.nc.util.JsonSupport;

/**
 * Demonstrates the use of dynamic templates in order to leverage full-text searching capabilities
 * using ngrams. See {@link TestKeywordThenText} for a more detailed explanation of dynamic
 * templates.
 *
 * @author cmuramoto
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestNoIndexedField {

	static class Graph {

		long gid;

		VO head;

		VO tail;

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			Graph other = (Graph) obj;
			if (gid != other.gid) {
				return false;
			}
			if (head == null) {
				if (other.head != null) {
					return false;
				}
			} else if (!head.equals(other.head)) {
				return false;
			}
			if (tail == null) {
				if (other.tail != null) {
					return false;
				}
			} else if (!tail.equals(other.tail)) {
				return false;
			}
			return true;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (gid ^ gid >>> 32);
			result = prime * result + (head == null ? 0 : head.hashCode());
			result = prime * result + (tail == null ? 0 : tail.hashCode());
			return result;
		}

		@Override
		public String toString() {
			return JsonSupport.toPrettyJson(this);
		}

	}

	static class VO {
		String id;

		String description;

		String summary;

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final VO other = (VO) obj;
			if (description == null) {
				if (other.description != null) {
					return false;
				}
			} else if (!description.equals(other.description)) {
				return false;
			}
			if (id == null) {
				if (other.id != null) {
					return false;
				}
			} else if (!id.equals(other.id)) {
				return false;
			}
			if (summary == null) {
				if (other.summary != null) {
					return false;
				}
			} else if (!summary.equals(other.summary)) {
				return false;
			}
			return true;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (description == null ? 0 : description.hashCode());
			result = prime * result + (id == null ? 0 : id.hashCode());
			result = prime * result + (summary == null ? 0 : summary.hashCode());
			return result;
		}

	}

	private static final String IX = "mixed";

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		String s = "{\"mixed_dyn\": {\"settings\": {\"index\": {				        \"creation_date\": \"1515286625351\",    \"provided_name\": \"mixed_dyn\"	      }				    }				  }				}";

		HashMap<String, Object> m = JsonSupport.fromJson(HashMap.class, s);

		System.out.println(m = (HashMap<String, Object>) m.get("mixed_dyn"));
		System.out.println(m = (HashMap<String, Object>) m.get("settings"));
		System.out.println(m = (HashMap<String, Object>) m.get("index"));
		Settings value = JsonSupport.mapper().convertValue(m, Settings.class);

		System.out.println(JsonSupport.toPrettyJson(value));
	}

	@Autowired
	IElasticSearchOps ops;

	@Test
	public void _001_queryShallowIndexed() {

		final ITypedBound<VO> bound = ops.bind(IX, "sample").typed(VO.class);

		final VO vo = new VO();

		vo.id = "ID";
		vo.description = "desc";
		vo.summary = "summary";

		final Result res = bound.insert(vo);
		Assert.assertSame(Status.created, res.result);

		bound.wrapped().refresh();

		MappedSearchResult<VO> query = bound.query(Term.term("id", "ID").asRoot());
		Assert.assertEquals(1, query.size());
		Assert.assertEquals(vo, query.first());

		query = bound.query(Term.term("summary", "summary").asRoot());
		Assert.assertEquals(1, query.size());
		Assert.assertEquals(vo, query.first());
	}

	@Test(expected = ElasticSearchOperationException.class)
	public void _002_queryShallowNonIndexed() {
		final ITypedBound<VO> bound = ops.bind(IX, "sample").typed(VO.class);
		MappedSearchResult<VO> query = bound.query(Term.term("description", "desc").asRoot());
		Assert.assertEquals(0, query.size());
	}

	@Test
	public void _003_queryDeepIndexed() {

		final ITypedBound<Graph> bound = ops.bind(IX, "sample").typed(Graph.class);

		final Graph g = new Graph();

		g.gid = Long.MAX_VALUE;
		VO head = new VO();

		head.id = "head_id";
		head.description = "head_desc";
		head.summary = "head_summary";

		VO tail = new VO();

		tail.id = "tail_id";
		tail.description = "tail_desc";
		tail.summary = "tail_summary";

		g.head = head;
		g.tail = tail;

		final Result res = bound.insert(g);
		Assert.assertSame(Status.created, res.result);

		bound.wrapped().refresh();

		MappedSearchResult<Graph> query = bound.query(Range.range("gid").exact(Long.MAX_VALUE).asRoot());
		Assert.assertEquals(query.size(), 1);
		Assert.assertEquals(g, query.first());

		query = bound.query(Term.term("head.id", "head_id").asRoot());
		Assert.assertEquals(1, query.size());
		Assert.assertEquals(g, query.first());

		query = bound.query(Term.term("head.summary", "head_summary").asRoot());
		Assert.assertEquals(1, query.size());
		Assert.assertEquals(g, query.first());
	}

	@Test(expected = ElasticSearchOperationException.class)
	public void _004_queryDeepNonIndexed() {
		final ITypedBound<Graph> bound = ops.bind(IX, "sample").typed(Graph.class);
		MappedSearchResult<Graph> query = bound.query(Term.term("head.description", "head_desc").asRoot());
		Assert.assertEquals(0, query.size());
	}

	@Test(expected = ElasticSearchOperationException.class)
	public void _005_queryDeepNonIndexed() {
		final ITypedBound<Graph> bound = ops.bind(IX, "sample").typed(Graph.class);
		MappedSearchResult<Graph> query = bound.query(Term.term("tail.id", "tail_id").asRoot());
		Assert.assertEquals(0, query.size());
	}

	@Test(expected = ElasticSearchOperationException.class)
	public void _006_queryDeepNonIndexed() {
		final ITypedBound<Graph> bound = ops.bind(IX, "sample").typed(Graph.class);
		MappedSearchResult<Graph> query = bound.query(Term.term("tail.summary", "tail_summary").asRoot());
		Assert.assertEquals(0, query.size());
	}

	@Test(expected = ElasticSearchOperationException.class)
	public void _007_queryDeepNonIndexed() {
		final ITypedBound<Graph> bound = ops.bind(IX, "sample").typed(Graph.class);
		MappedSearchResult<Graph> query = bound.query(Term.term("tail.description", "tail_desc").asRoot());
		Assert.assertEquals(0, query.size());
	}

	@Before
	public void setup() {
		if (ops.exists(IX)) {
			Settings s = ops.settings(IX);

			if (s != null && s.creationDate() > 0 && System.currentTimeMillis() - s.creationDate() < 30_000) {
				return;
			}
		}

		final String index = IX;
		ops.deleteIfExists(index);

		// final DynamicTemplate indexed_objects =
		// DynamicTemplate.of(GeneralType.OBJECT).useJavaRegex().unMatching("(*NON_IXOBJ_.+)");
		// final DynamicTemplate non_indexed_objects =
		// DynamicTemplate.of(null).useJavaRegex().matching("(NON_IX_.+|NON_IXOBJ_.+|NON_IXOBJ_TAIL.summary)").mappingTo(Property.get().noIndex().stored());

		// final DynamicTemplate indexed_strings =
		// DynamicTemplate.of(GeneralType.STRING).useJavaRegex().unMatching("(NON_IX_.+|NON_IXOBJ_.+|NON_IXOBJ_TAIL.summary)").mappingTo(Property.get().with(Type.KEYWORD));
		// final DynamicTemplate non_indexed_strings =
		// DynamicTemplate.of(null).useJavaRegex().matching("(NON_IX_.+)").mappingTo(Property.get().noIndex().stored());

		Property key_word = Property.get().with(Type.KEYWORD);
		// Property obj = Property.get().with(Type.OBJECT);
		Property non_indexed = Property.get().with(Type.KEYWORD).noIndex().stored();

		Property head = Property.get().add("id", key_word).add("summary", key_word).add("description", non_indexed);
		Property tail = Property.get().add("id", non_indexed).add("summary", non_indexed).add("description", non_indexed);

		final IndexDefinition def = IndexDefinition.get().with(Mappings.get().add("doc", IndexedType.get().disableAll()//
				.add("id", key_word).add("summary", key_word).add("description", non_indexed)//
				.add("head", head)//
				.add("tail", tail)//
		));
		ops.createIndex(index, def);

	}

}
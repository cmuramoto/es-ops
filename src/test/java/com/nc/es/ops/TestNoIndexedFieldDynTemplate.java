package com.nc.es.ops;

import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.nc.es.api.ElasticSearchOperationException;
import com.nc.es.api.IElasticSearchOps;
import com.nc.es.api.IElasticSearchOps.ITypedBound;
import com.nc.es.config.DynamicTemplate;
import com.nc.es.config.IndexDefinition;
import com.nc.es.config.IndexedType;
import com.nc.es.config.Mappings;
import com.nc.es.config.Property;
import com.nc.es.config.Property.GeneralType;
import com.nc.es.config.Property.Type;
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
public class TestNoIndexedFieldDynTemplate {

	static class Graph {

		long gid;

		VO head;

		@JsonProperty("no_ix_tail")
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

		@JsonProperty("no_ix_desc")
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

	static boolean initialized;

	private static final String IX = "mixed_dyn";

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
		MappedSearchResult<VO> query = bound.query(Term.term("no_ix_desc", "desc").asRoot());
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
		MappedSearchResult<Graph> query = bound.query(Term.term("head.no_ix_desc", "head_desc").asRoot());
		Assert.assertEquals(0, query.size());
	}

	@Test(expected = ElasticSearchOperationException.class)
	public void _005_queryDeepNonIndexed() {
		final ITypedBound<Graph> bound = ops.bind(IX, "sample").typed(Graph.class);
		MappedSearchResult<Graph> query = bound.query(Term.term("no_ix_tail.id", "tail_id").asRoot());
		Assert.assertEquals(0, query.size());
	}

	@Test(expected = ElasticSearchOperationException.class)
	public void _006_queryDeepNonIndexed() {
		final ITypedBound<Graph> bound = ops.bind(IX, "sample").typed(Graph.class);
		MappedSearchResult<Graph> query = bound.query(Term.term("no_ix_tail.summary", "tail_summary").asRoot());
		Assert.assertEquals(0, query.size());
	}

	@Test(expected = ElasticSearchOperationException.class)
	public void _007_queryDeepNonIndexed() {
		final ITypedBound<Graph> bound = ops.bind(IX, "sample").typed(Graph.class);
		MappedSearchResult<Graph> query = bound.query(Term.term("no_ix_tail.no_ix_desc", "tail_desc").asRoot());
		Assert.assertEquals(0, query.size());
	}

	@Before
	public void setup() {
		if (!initialized) {
			final String index = IX;
			ops.deleteIfExists(index);

			final DynamicTemplate indexed_strings = DynamicTemplate.of(GeneralType.STRING).pathUnMatching("*no_ix_*").mappingTo(Property.get().with(Type.KEYWORD));
			final DynamicTemplate non_indexed_strings = DynamicTemplate.of(null).pathMatching("*no_ix_*").mappingTo(Property.get().noIndex());

			final IndexDefinition def = IndexDefinition.get().with(Mappings.get().withDefaults(IndexedType.get().disableAll().with("non_indexed_strings", non_indexed_strings).with("indexed_strings", indexed_strings)));

			ops.createIndex(index, def);

			initialized = true;
		}
	}

}
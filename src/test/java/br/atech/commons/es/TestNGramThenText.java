package br.atech.commons.es;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.nc.es.api.IElasticSearchOps;
import com.nc.es.api.IElasticSearchOps.ITypedBound;
import com.nc.es.config.DynamicTemplate;
import com.nc.es.config.IndexDefinition;
import com.nc.es.config.IndexedType;
import com.nc.es.config.Mappings;
import com.nc.es.config.Property;
import com.nc.es.config.Settings;
import com.nc.es.config.Property.GeneralType;
import com.nc.es.config.Property.Type;
import com.nc.es.ops.MappedSearchResult;
import com.nc.es.ops.Result;
import com.nc.es.ops.Result.Status;
import com.nc.es.search.ILeafQuery.Match;
import com.nc.es.search.ILeafQuery.Term;

/**
 * Demonstrates the use of dynamic templates in order to leverage full-text searching capabilities
 * using ngrams. See {@link TestKeywordThenText} for a more detailed explanation of dynamic
 * templates.
 *
 * @author cmuramoto
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestNGramThenText {

	static class VO {
		String id;

		@JsonProperty("COMBO_DESC")
		String description;

		@JsonProperty("TXT_SUMMARY")
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

	private static final String IX = "ngram_then_text";

	@Autowired
	IElasticSearchOps ops;

	@Test
	public void run() {

		final ITypedBound<VO> bound = ops.bind(IX, "sample").typed(VO.class);

		final VO vo = new VO();

		vo.id = "ID";
		vo.description = "A large text description including ipsum lorem";
		vo.summary = "A short summary";

		final Result res = bound.insert(vo);
		// 6.x doesn't report found/created anymore
		// Assert.assertEquals(true, res.created);
		Assert.assertSame(Status.created, res.result);

		bound.wrapped().refresh();

		// Structured text. The id is mapped to a keyword only
		MappedSearchResult<VO> query = bound.query(Term.term("id", "ID").asRoot());
		Assert.assertEquals(query.size(), 1);
		Assert.assertEquals(query.first(), vo);

		// Full text. The main field is analyzed
		query = bound.query(Term.term("COMBO_DESC", "large").asRoot());
		Assert.assertEquals(1, query.size());
		Assert.assertEquals(vo, query.first());

		// Structured text. The sub-field is a keyword
		query = bound.query(Term.term("COMBO_DESC.keyword", "A large text description including ipsum lorem").asRoot());
		Assert.assertEquals(1, query.size());
		Assert.assertEquals(vo, query.first());

		// Structured text. The main field is a keyword
		query = bound.query(Term.term("TXT_SUMMARY", "A short summary").asRoot());
		Assert.assertEquals(1, query.size());
		Assert.assertEquals(vo, query.first());

		// Full text. The sub field is analyzed
		query = bound.query(Term.term("TXT_SUMMARY.ngrams", "short").asRoot());
		Assert.assertEquals(1, query.size());
		Assert.assertEquals(vo, query.first());

		// Full text. The main field is analyzed
		query = bound.query(Term.term("COMBO_DESC", "A large text description including ipsum lorem").asRoot());
		Assert.assertEquals(0, query.size());

		// Structured text. The main field is a keyword
		query = bound.query(Term.term("TXT_SUMMARY", "short").asRoot());
		Assert.assertEquals(0, query.size());

		// Now lets have some fun
		query = bound.query(Match.query("TXT_SUMMARY.ngrams", "vacaary").analyzer("ngram").asRoot());
		Assert.assertEquals(1, query.size());

	}

	@Before
	public void setup() {
		final String index = IX;
		ops.deleteIfExists(index);

		final Settings s = Settings.get().installNGram("ngram", 2, 10, false, true);

		final DynamicTemplate keywords = DynamicTemplate.of(GeneralType.STRING).useJavaRegex().unMatching("(TXT_.+|COMBO_.+)").mappingTo(Property.get().with(Type.KEYWORD));
		final DynamicTemplate keywordsAndText = DynamicTemplate.of(GeneralType.STRING).useJavaRegex().matching("TXT_.+").mappingTo(Property.get().with(Type.KEYWORD).withSubField("ngrams", Property.get().with(Type.TEXT).analyzedWith("ngram").searchedWith("standard")));

		final IndexDefinition def = IndexDefinition.get().with(s).with(Mappings.get().withDefaults(IndexedType.get().disableAll().with("keywords_only", keywords).with("keywords_then_txt", keywordsAndText)));
		ops.createIndex(index, def);
	}

}
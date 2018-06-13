package com.nc.es.ops;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.locks.LockSupport;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.nc.es.api.IElasticSearchObject;
import com.nc.es.api.IElasticSearchOps;
import com.nc.es.api.IElasticSearchOps.IBound;
import com.nc.es.config.DynamicTemplate;
import com.nc.es.config.IndexDefinition;
import com.nc.es.config.IndexedType;
import com.nc.es.config.Mappings;
import com.nc.es.config.Property;
import com.nc.es.config.Property.GeneralType;
import com.nc.es.config.Property.Type;
import com.nc.es.ops.BulkDeleteResult;
import com.nc.es.ops.MappedSearchResult;
import com.nc.es.search.RootQuery;
import com.nc.es.search.ICompoundQuery.Bool;
import com.nc.es.search.ILeafQuery.Range;
import com.nc.es.search.ILeafQuery.Term;

/**
 * _ttl and _timestamp have been deprecated. As per documentation, one should configure this feature
 * at application level by creating a timestamp field and running a delete by query periodically.
 *
 * @author cmuramoto
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestBoolean {

	// Overriding custom precise serializer
	static class LocalDateTimeDeserializer extends JsonDeserializer<LocalDateTime> {

		@Override
		public LocalDateTime deserialize(JsonParser p, DeserializationContext ctx) throws IOException, JsonProcessingException {
			return LocalDateTime.parse(p.getText(), FMT);
		}
	}

	static class LocalDateTimeSerializer extends JsonSerializer<LocalDateTime> {
		@Override
		public void serialize(LocalDateTime t, JsonGenerator g, SerializerProvider p) throws IOException, JsonProcessingException {
			g.writeString(t.format(FMT));
		}
	}

	static class VO implements IElasticSearchObject {

		@JsonSerialize(using = LocalDateTimeSerializer.class)
		@JsonDeserialize(using = LocalDateTimeDeserializer.class)
		LocalDateTime timestamp = LocalDateTime.now();

		String label;

		String text;
	}

	static DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

	@Autowired
	IElasticSearchOps ops;

	IBound bound;

	@Test
	public void run() {

		// E.g. 10:10:00
		final VO vo = new VO();
		vo.label = "Test";
		vo.text = "sample";
		bound.insert(vo);

		bound.refresh();

		final Logger log = LoggerFactory.getLogger(getClass());
		log.info(vo.asPrettyJson());

		RootQuery root = Bool.create().filter(Term.term("label", "Test")).mustNot(Bool.create().filter(Term.term("text", "sample"))).asRoot();

		MappedSearchResult<VO> query = bound.query(VO.class, root);
		Assert.assertTrue(query.isEmpty());

		// Created before or at 10:09:58
		final String fmt = "yyyy-MM-dd'T'HH:mm:ss";

		root = Bool.create().filter(Term.term("label", "Test")).must(Bool.create().must(Term.term("text", "sample")).filter(Range.range("timestamp").format(fmt).lte(LocalDateTime.now().minusSeconds(2)))).asRoot();
		BulkDeleteResult delete = bound.delete(root);
		Assert.assertEquals(0, delete.total);
		bound.refresh();

		LockSupport.parkUntil(System.currentTimeMillis() + 2000);

		// Created before or at 10:10:00
		root = Bool.create().filter(Term.term("label", "Test")).must(Bool.create().must(Term.term("text", "sample")).filter(Range.range("timestamp").format(fmt).lte(LocalDateTime.now().minusSeconds(2)))).asRoot();

		final VO rec = bound.query(VO.class, root).hits().iterator().next().source;
		log.info(rec.asPrettyJson());

		delete = bound.delete(root);
		Assert.assertEquals(1, delete.total);
		bound.refresh();

		query = bound.query(VO.class, root);

		Assert.assertTrue(query.isEmpty());
	}

	@Before
	public void setup() {
		final IndexDefinition def = IndexDefinition.get().with(Mappings.get().withDefaults(IndexedType.get().disableAll().autoDetectingDatesWith("yyyy/MM/dd", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS").with("only_keywords", DynamicTemplate.of(GeneralType.STRING).mappingTo(Property.get().with(Type.KEYWORD)))));
		final String boolIx = "bool_ix";
		if (ops.exists(boolIx)) {
			ops.deleteIndex(boolIx);
		}
		ops.createIndex(boolIx, def);

		bound = ops.bind(boolIx, "vo");
		bound.delete(RootQuery.matchAll());
	}
}
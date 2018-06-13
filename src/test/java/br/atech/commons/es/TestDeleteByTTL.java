package br.atech.commons.es;

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

/**
 * _ttl and _timestamp have been deprecated. As per documentation, one should configure this feature
 * at application level by creating a timestamp field and running a delete by query periodically.
 *
 * @author cmuramoto
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestDeleteByTTL {

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
		bound.insert(vo);

		bound.refresh();

		final Logger log = LoggerFactory.getLogger(getClass());
		log.info(vo.asPrettyJson());

		final RootQuery term = RootQuery.term("label", "Test");
		MappedSearchResult<VO> query = bound.query(VO.class, term);
		Assert.assertFalse(query.isEmpty());
		VO rec = query.hits().iterator().next().source;
		log.info(rec.asPrettyJson());

		// Created before or at 10:09:58
		RootQuery dateLow = RootQuery.dateLow("timestamp", LocalDateTime.now().minusSeconds(2), "yyyy-MM-dd'T'HH:mm:ss");
		BulkDeleteResult delete = bound.delete(dateLow);
		Assert.assertEquals(0, delete.total);
		bound.refresh();

		rec = bound.query(VO.class, term).hits().iterator().next().source;
		log.info(rec.asPrettyJson());

		LockSupport.parkUntil(System.currentTimeMillis() + 2000);

		// Created before or at 10:10:00
		dateLow = RootQuery.dateLow("timestamp", LocalDateTime.now().minusSeconds(2), "yyyy-MM-dd'T'HH:mm:ss");
		delete = bound.delete(dateLow);
		Assert.assertEquals(1, delete.total);
		bound.refresh();

		query = bound.query(VO.class, term);

		Assert.assertTrue(query.isEmpty());

	}

	@Before
	public void setup() {
		final IndexDefinition def = IndexDefinition.get().with(Mappings.get().withDefaults(IndexedType.get().disableAll().autoDetectingDatesWith("yyyy/MM/dd", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS").with("only_keywords", DynamicTemplate.of(GeneralType.STRING).mappingTo(Property.get().with(Type.KEYWORD)))));
		if (ops.exists("test_ttl")) {
			ops.deleteIndex("test_ttl");
		}
		ops.createIndex("test_ttl", def);

		bound = ops.bind("test_ttl", "vo");
		bound.delete(RootQuery.matchAll());
	}
}